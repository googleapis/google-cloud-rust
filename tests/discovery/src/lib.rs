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

use anyhow::Result;
use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
use google_cloud_compute_v1::client::{
    Images, InstanceGroups, Instances, MachineTypes, RegionHealthChecks, Zones,
};
use google_cloud_compute_v1::model::{
    AttachedDisk, AttachedDiskInitializeParams, HealthCheck, Instance, InstanceGroup,
    NetworkInterface, ServiceAccount, TCPHealthCheck, health_check,
};
use google_cloud_gax::error::rpc::{Code, StatusDetails};
use google_cloud_gax::paginator::ItemPaginator as _;
use google_cloud_gax::retry_policy::{Aip194Strict, RetryPolicy, RetryPolicyExt};
use google_cloud_lro::Poller;
use google_cloud_test_utils::resource_names::{PREFIX, random_image_name, random_vm_id};
use google_cloud_test_utils::runtime_config::{
    project_id, region_id, test_service_account, zone_id,
};
use google_cloud_wkt::Timestamp;
use std::time::Duration;

fn retry_policy() -> impl RetryPolicy {
    Aip194Strict
        .continue_on_client_timeout()
        .continue_on_too_many_requests()
        .with_time_limit(5 * Duration::from_secs(60))
}

pub async fn zones() -> Result<()> {
    let client = Zones::builder()
        .with_tracing()
        .with_retry_policy(retry_policy())
        .build()
        .await?;
    let project_id = project_id()?;
    let zone_id = zone_id();

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
        Some(google_cloud_compute_v1::model::zone::Status::Up),
        "response={response:?}"
    );
    tracing::info!("Zones::get() = {response:?}");

    Ok(())
}

pub async fn errors() -> Result<()> {
    let project_id = project_id()?;
    let zone_id = zone_id();

    let credentials = Anonymous::new().build();
    let client = Zones::builder()
        .with_credentials(credentials)
        .with_retry_policy(retry_policy())
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

    let client = Zones::builder()
        .with_retry_policy(retry_policy())
        .build()
        .await?;
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
    let client = MachineTypes::builder()
        .with_tracing()
        .with_retry_policy(retry_policy())
        .build()
        .await?;
    let project_id = project_id()?;
    let zone_id = zone_id();

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
        tracing::info!(
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

pub async fn cleanup_stale_images(client: &Images, project_id: &str) -> Result<()> {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    let stale_deadline = SystemTime::now().duration_since(UNIX_EPOCH)?;
    let stale_deadline = stale_deadline - Duration::from_secs(48 * 60 * 60);
    let stale_deadline = Timestamp::clamp(stale_deadline.as_secs() as i64, 0);

    let mut items = client.list().set_project(project_id).by_item();
    while let Some(item) = items.next().await.transpose()? {
        if item
            .labels
            .get("integration-test")
            .is_none_or(|v| v != "true")
        {
            continue;
        }
        if item
            .creation_timestamp
            .and_then(|v| Timestamp::try_from(&v).ok())
            .is_none_or(|t| t > stale_deadline)
        {
            continue;
        }
        let Some(name) = item.name else {
            continue;
        };
        let _ = client
            .delete()
            .set_project(project_id)
            .set_image(name.clone())
            .poller()
            .until_done()
            .await
            .inspect_err(|e| eprintln!("error cleaning up image {name}: {e:?}"))
            .map(|op| {
                op.to_result()
                    .inspect_err(|e| eprintln!("error cleaning up image {name}: {e:?}"))
            });
    }
    Ok(())
}

pub async fn images() -> Result<()> {
    use google_cloud_compute_v1::model::Image;
    use rand::seq::IndexedRandom;

    let project_id = project_id()?;
    let client = Images::builder().with_tracing().build().await?;
    if let Err(e) = cleanup_stale_images(&client, &project_id).await {
        tracing::error!("Error cleaning up stale test images: {e:?}");
    };

    tracing::info!("Testing Images::list()");
    // The test project cannot copy Debian or other "untrusted" images.
    let mut items = client
        .list()
        .set_project("cos-cloud")
        .set_filter("family=cos-stable AND architecture=X86_64")
        .by_item();
    // Pick an image at random. The quota limit for copying
    // images is too low for testing.
    let mut candidates = Vec::new();
    while let Some(item) = items.next().await.transpose()? {
        tracing::info!("item = {item:?}");
        if let Some(name) = item.name {
            candidates.push(name);
        }
    }
    tracing::info!("DONE with Images::list()");
    let found = candidates
        .choose(&mut rand::rng())
        .expect("at least one COS image should be available");

    tracing::info!("Testing Images::insert()");
    let name = random_image_name();
    let body = Image::new()
        .set_name(&name)
        .set_description("A test Image created by the Rust client library.")
        .set_family("cos-stable")
        .set_labels([("integration-test", "true")])
        // Explicitly set a valid storage location (`"us"`) so GCE does not try
        // to infer a multi-region from source images replicated in regions like
        // `africa-south1` (which fails with `Invalid value for storage location: AFRICA`).
        .set_storage_locations(["us"])
        .set_source_image(format!("projects/cos-cloud/global/images/{found}"));
    let operation = client
        .insert()
        .set_project(&project_id)
        .set_body(body)
        .poller()
        .until_done()
        .await?
        .to_result()?;
    tracing::info!("Images::insert() finished with {operation:?}");

    tracing::info!("Testing Images::delete()");
    let operation = client
        .delete()
        .set_project(&project_id)
        .set_image(&name)
        .poller()
        .until_done()
        .await?
        .to_result()?;
    tracing::info!("Images::delete() completed with {operation:?}");

    Ok(())
}

pub async fn cleanup_stale_instance_groups(
    client: &InstanceGroups,
    project_id: &str,
    zone_id: &str,
) -> Result<()> {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    let stale_deadline = SystemTime::now().duration_since(UNIX_EPOCH)?;
    let stale_deadline = stale_deadline - Duration::from_secs(48 * 60 * 60);
    let stale_deadline = Timestamp::clamp(stale_deadline.as_secs() as i64, 0);

    let mut items = client
        .list()
        .set_project(project_id)
        .set_zone(zone_id)
        .by_item();
    while let Some(item) = items.next().await.transpose()? {
        let Some(name) = item.name.filter(|n| n.starts_with(PREFIX)) else {
            continue;
        };
        if item
            .creation_timestamp
            .and_then(|v| Timestamp::try_from(&v).ok())
            .is_none_or(|t| t > stale_deadline)
        {
            continue;
        }
        let _ = client
            .delete()
            .set_project(project_id)
            .set_zone(zone_id)
            .set_instance_group(&name)
            .poller()
            .until_done()
            .await
            .inspect_err(|e| eprintln!("error cleaning up instance group {name}: {e:?}"))
            .map(|op| {
                op.to_result()
                    .inspect_err(|e| eprintln!("error cleaning up instance group {name}: {e:?}"))
            });
    }
    Ok(())
}

pub async fn instance_groups() -> Result<()> {
    let client = InstanceGroups::builder()
        .with_tracing()
        .with_retry_policy(retry_policy())
        .build()
        .await?;
    let project_id = project_id()?;
    let zone_id = zone_id();

    if let Err(e) = cleanup_stale_instance_groups(&client, &project_id, &zone_id).await {
        tracing::error!("Error cleaning up stale test instance groups: {e:?}");
    }

    let id = random_vm_id();
    let body = InstanceGroup::new()
        .set_name(&id)
        .set_description("A test InstanceGroup created by the Rust client library.");

    tracing::info!("Testing InstanceGroups::insert()");
    let operation = client
        .insert()
        .set_project(&project_id)
        .set_zone(&zone_id)
        .set_body(body)
        .poller()
        .until_done()
        .await?
        .to_result()?;
    tracing::info!("InstanceGroups::insert() completed with = {operation:?}");

    tracing::info!("Testing InstanceGroups::get()");
    let group = client
        .get()
        .set_project(&project_id)
        .set_zone(&zone_id)
        .set_instance_group(&id)
        .send()
        .await?;
    assert_eq!(group.name.as_deref(), Some(id.as_str()), "group={group:?}");
    tracing::info!("group = {group:?}");

    tracing::info!("Testing InstanceGroups::list()");
    let mut items = client
        .list()
        .set_project(&project_id)
        .set_zone(&zone_id)
        .by_item();
    let mut found = false;
    while let Some(item) = items.next().await.transpose()? {
        tracing::info!("item = {item:?}");
        if item.name.as_deref() == Some(id.as_str()) {
            found = true;
        }
    }
    assert!(found, "expected to find instance group {id} in list()");
    tracing::info!("DONE InstanceGroups::list()");

    tracing::info!("Testing InstanceGroups::delete()");
    let operation = client
        .delete()
        .set_project(&project_id)
        .set_zone(&zone_id)
        .set_instance_group(&id)
        .poller()
        .until_done()
        .await?
        .to_result()?;
    tracing::info!("InstanceGroups::delete() completed with = {operation:?}");

    Ok(())
}

pub async fn lro_errors() -> Result<()> {
    let client = Instances::builder().with_tracing().build().await?;
    let project_id = project_id()?;
    let service_account = test_service_account()?;
    let zone_id = zone_id();
    // A machine type for which the project does not have enough quota.
    const MACHINE_TYPE: &str = "c4d-standard-384";

    let id = crate::random_vm_id();
    let body = Instance::new()
        .set_machine_type(format!("zones/{zone_id}/machineTypes/{MACHINE_TYPE}"))
        .set_name(&id)
        .set_description("A test VM created by the Rust client library.")
        .set_labels([("integration-test", "true")])
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
    let err = operation
        .to_result()
        .expect_err("the VM creation request should fail due to lack of quota");
    tracing::info!("Expected error: {err:?}");
    use google_cloud_compute_v1::errors::OperationError;
    let OperationError::Generic(generic) = err else {
        panic!("expected a OperationError::Generic(_) {err:?}")
    };
    let Some(detail) = generic.details.as_ref().and_then(|e| e.errors.first()) else {
        panic!("expected at least one error details in {generic:?}");
    };
    assert!(detail.code.is_some(), "{detail:?}");
    assert!(!detail.error_details.is_empty(), "{detail:?}");

    Ok(())
}

pub async fn cleanup_stale_region_health_checks(
    client: &RegionHealthChecks,
    project_id: &str,
    region_id: &str,
) -> Result<()> {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    let stale_deadline = SystemTime::now().duration_since(UNIX_EPOCH)?;
    let stale_deadline = stale_deadline - Duration::from_secs(48 * 60 * 60);
    let stale_deadline = Timestamp::clamp(stale_deadline.as_secs() as i64, 0);

    let mut items = client
        .list()
        .set_project(project_id)
        .set_region(region_id)
        .by_item();
    while let Some(item) = items.next().await.transpose()? {
        let Some(name) = item.name.filter(|n| n.starts_with(PREFIX)) else {
            continue;
        };
        if item
            .creation_timestamp
            .and_then(|v| Timestamp::try_from(&v).ok())
            .is_none_or(|t| t > stale_deadline)
        {
            continue;
        }
        let _ = client
            .delete()
            .set_project(project_id)
            .set_region(region_id)
            .set_health_check(&name)
            .poller()
            .until_done()
            .await
            .inspect_err(|e| eprintln!("error cleaning up region health check {name}: {e:?}"))
            .map(|op| {
                op.to_result().inspect_err(|e| {
                    eprintln!("error cleaning up region health check {name}: {e:?}")
                })
            });
    }
    Ok(())
}

pub async fn region_health_checks() -> Result<()> {
    let client = RegionHealthChecks::builder()
        .with_tracing()
        .with_retry_policy(retry_policy())
        .build()
        .await?;
    let project_id = project_id()?;
    let region_id = region_id();

    if let Err(e) = cleanup_stale_region_health_checks(&client, &project_id, &region_id).await {
        tracing::error!("Error cleaning up stale test region health checks: {e:?}");
    }

    let id = random_vm_id();
    let body = HealthCheck::new()
        .set_name(&id)
        .set_description("A test HealthCheck created by the Rust client library.")
        .set_type(health_check::Type::Tcp)
        .set_tcp_health_check(TCPHealthCheck::new().set_port(80));

    tracing::info!("Testing RegionHealthChecks::insert()");
    let operation = client
        .insert()
        .set_project(&project_id)
        .set_region(&region_id)
        .set_body(body)
        .poller()
        .until_done()
        .await?
        .to_result()?;
    tracing::info!("RegionHealthChecks::insert() completed with = {operation:?}");

    tracing::info!("Testing RegionHealthChecks::get()");
    let health_check = client
        .get()
        .set_project(&project_id)
        .set_region(&region_id)
        .set_health_check(&id)
        .send()
        .await?;
    assert_eq!(
        health_check.name.as_deref(),
        Some(id.as_str()),
        "health_check={health_check:?}"
    );
    tracing::info!("health_check = {health_check:?}");

    tracing::info!("Testing RegionHealthChecks::list()");
    let mut items = client
        .list()
        .set_project(&project_id)
        .set_region(&region_id)
        .by_item();
    let mut found = false;
    while let Some(item) = items.next().await.transpose()? {
        tracing::info!("item = {item:?}");
        if item.name.as_deref() == Some(id.as_str()) {
            found = true;
        }
    }
    assert!(found, "expected to find region health check {id} in list()");
    tracing::info!("DONE RegionHealthChecks::list()");

    tracing::info!("Testing RegionHealthChecks::delete()");
    let operation = client
        .delete()
        .set_project(&project_id)
        .set_region(&region_id)
        .set_health_check(&id)
        .poller()
        .until_done()
        .await?
        .to_result()?;
    tracing::info!("RegionHealthChecks::delete() completed with = {operation:?}");

    Ok(())
}
