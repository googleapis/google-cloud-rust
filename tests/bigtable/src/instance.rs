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

use anyhow::Result;
use google_cloud_bigtable_admin_v2::client::BigtableInstanceAdmin;
use google_cloud_bigtable_admin_v2::model::{Cluster, Instance, StorageType};
use google_cloud_lro::Poller as _;
use google_cloud_test_utils::resource_names::{LowercaseAlphanumeric, PREFIX};
use std::time::{Duration, SystemTime};

const MAX_STALE: Duration = Duration::from_secs(48 * 3600);

pub async fn create_test_instance(
    client: &BigtableInstanceAdmin,
    project: &str,
    zone: &str,
) -> Result<Instance> {
    let instance_id = random_instance_id();
    tracing::info!("creating test instance {instance_id} in {project} ({zone})");
    let instance = client
        .create_instance()
        .set_parent(format!("projects/{project}"))
        .set_instance_id(&instance_id)
        .set_instance(
            Instance::new()
                .set_display_name(&instance_id)
                .set_labels([("integration-test", "true")]),
        )
        .set_clusters([(
            format!("{instance_id}-c1"),
            Cluster::new()
                .set_location(format!("projects/{project}/locations/{zone}"))
                .set_serve_nodes(1)
                .set_default_storage_type(StorageType::Ssd),
        )])
        .poller()
        .until_done()
        .await?;
    tracing::info!("created instance: {}", instance.name);
    Ok(instance)
}

pub async fn delete_instance(client: &BigtableInstanceAdmin, name: &str) {
    tracing::info!("deleting instance: {name}");
    if let Err(e) = client.delete_instance().set_name(name).send().await {
        tracing::warn!("error deleting instance {name}: {e:?}");
    }
}

pub async fn cleanup_stale_instances(client: &BigtableInstanceAdmin, project: &str) -> Result<()> {
    let deadline = SystemTime::now() - MAX_STALE;
    let deadline = wkt::Timestamp::try_from(deadline)?;

    let response = client
        .list_instances()
        .set_parent(format!("projects/{project}"))
        .send()
        .await?;

    let mut stale = Vec::new();
    for instance in response.instances {
        if instance
            .labels
            .get("integration-test")
            .is_none_or(|v| v != "true")
        {
            continue;
        }
        if instance
            .create_time
            .is_some_and(|c| c.seconds() < deadline.seconds())
        {
            stale.push(instance.name);
        }
    }

    let deletes = stale
        .into_iter()
        .map(|name| client.delete_instance().set_name(name).send());
    for res in futures::future::join_all(deletes).await {
        if let Err(e) = res {
            tracing::warn!("error deleting stale instance: {e:?}");
        }
    }
    Ok(())
}

fn random_instance_id() -> String {
    // Bigtable instance IDs must be 6-33 chars, cluster IDs up to 30 chars.
    let id = LowercaseAlphanumeric.random_string(10);
    format!("{PREFIX}{id}")
}
