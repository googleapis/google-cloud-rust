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
use futures::stream::{StreamExt, TryStreamExt};
use gax::paginator::ItemPaginator;
use rand::Rng;
use sql::model;
use storage_samples::RandomChars;

pub async fn run_sql_instances_service() -> Result<()> {
    let project_id = crate::project_id()?;
    let name = random_sql_instance_name(&project_id);
    let client = sql::client::SqlInstancesService::builder()
        .with_tracing()
        .build()
        .await?;

    cleanup_stale_sql_instances(&client, &project_id).await?;

    println!("\nTesting insert sql instance");
    let insert = client
        .insert()
        .set_project(&project_id)
        .set_body(
            model::DatabaseInstance::new().set_name(&name).set_settings(
                model::Settings::new()
                    .set_tier("db-f1-micro")
                    .set_user_labels([(INSTANCE_LABEL, "true")]),
            ),
        )
        .send()
        .await?;
    println!("SUCCESS on insert sql instance: {insert:?}");
    assert_eq!(insert.target_id, name);

    println!("Testing get sql instance");
    let get = client
        .get()
        .set_project(&project_id)
        .set_instance(&name)
        .send()
        .await?;
    println!("SUCCESS on get sql instance: {get:?}");
    assert_eq!(get.name, name);
    let settings = get
        .settings
        .as_ref()
        .ok_or_else(|| anyhow::Error::msg("settings should contain a value"))?;
    assert_eq!(settings.tier, "db-f1-micro");

    println!("Testing list sql instances");
    let list = client
        .list()
        .set_project(&project_id)
        .set_filter(format!("name:{name}"))
        .by_item()
        .into_stream();
    // We expect at most one result, so collect and pop yields the element.
    let found = list.try_collect::<Vec<_>>().await?.pop();
    println!("SUCCESS on list sql instance: {found:?}");
    // Sometimes the list returns no elements. We speculate this is because the
    // index of all instances is not updated fast enough. But if it contains an
    // element it should be the expected instance.
    assert!(found.as_ref().is_none_or(|d| d == &get), "{found:?}");

    println!("Testing delete sql instance");
    let delete = client
        .delete()
        .set_project(&project_id)
        .set_instance(&name)
        .send()
        .await?;
    println!("SUCCESS on delete sql instance: {delete:?}");
    assert_eq!(delete.target_id, name);

    Ok(())
}

const PREFIX: &str = "rust-sdk-testing-";
const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
pub const INSTANCE_NAME_LENGTH: usize = 98;
const INSTANCE_LABEL: &str = "rust-sdk-integration-test";

fn random_sql_instance_name(project_id: &str) -> String {
    let distr = RandomChars::new(CHARSET);
    let rand_suffix: String = rand::rng()
        .sample_iter(distr)
        .take(INSTANCE_NAME_LENGTH - project_id.len() - PREFIX.len() - 1) // project-ID:instance-ID <= 98
        .map(char::from)
        .collect();
    format!("{PREFIX}{rand_suffix}")
}

async fn cleanup_stale_sql_instances(
    client: &sql::client::SqlInstancesService,
    project_id: &str,
) -> Result<()> {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    let stale_deadline = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(anyhow::Error::from)?;
    let stale_deadline = stale_deadline - Duration::from_secs(48 * 60 * 60);

    let stale_deadline = wkt::Timestamp::clamp(stale_deadline.as_secs() as i64, 0);

    let instances = client
        .list()
        .set_project(project_id)
        .set_filter(format!(
            "name:{PREFIX}* AND settings.userLabels.{INSTANCE_LABEL}:true"
        ))
        .by_item()
        .into_stream();

    let pending_deletion = instances
        .filter_map(|instance| async {
            match instance {
                Ok(instance) => {
                    if instance.create_time? < stale_deadline {
                        Some(
                            client
                                .delete()
                                .set_project(project_id)
                                .set_instance(instance.name)
                                .send(),
                        )
                    } else {
                        None
                    }
                }
                Err(_) => None,
            }
        })
        .collect::<Vec<_>>()
        .await;

    futures::future::join_all(pending_deletion.into_iter())
        .await
        .into_iter()
        .for_each(|res| {
            if let Err(err) = res {
                println!("Cleanup error: deleting sql instance resulted in {err:?}")
            }
        });

    Ok(())
}

pub async fn run_sql_tiers_service() -> Result<()> {
    let project_id = crate::project_id()?;
    let client = sql::client::SqlTiersService::builder()
        .with_tracing()
        .build()
        .await?;

    let list = client.list().set_project(&project_id).send().await?;

    assert_ne!(
        list.items
            .into_iter()
            .find(|v| v.tier.eq("db-f1-micro"))
            .ok_or_else(|| anyhow::Error::msg("tiers list should contain db-f1-micro"))?
            .ram,
        0
    );

    Ok(())
}
