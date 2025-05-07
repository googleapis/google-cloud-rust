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

use crate::{Error, RandomChars, Result};
use rand::Rng;
use sql::model;

pub async fn run_sql_instances_service(
    builder: sql::builder::sql_instances_service::ClientBuilder,
) -> Result<()> {
    // Enable a basic subscriber. Useful to troubleshoot problems and visually
    // verify tracing is doing something.
    #[cfg(feature = "log-integration-tests")]
    let _guard = {
        use tracing_subscriber::fmt::format::FmtSpan;
        let subscriber = tracing_subscriber::fmt()
            .with_level(true)
            .with_thread_ids(true)
            .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
            .finish();

        tracing::subscriber::set_default(subscriber)
    };

    let project_id = crate::project_id()?;
    let name = random_sql_instance_name(&project_id);
    let client: sql::client::SqlInstancesService = builder.build().await?;

    cleanup_stale_sql_instances(&client, &project_id).await?;

    println!("\nTesting insert sql instance");
    let insert = client
        .insert(&project_id)
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
    let get = client.get(&project_id, &name).send().await?;
    println!("SUCCESS on get sql instance: {get:?}");
    assert_eq!(get.name, name);
    let settings = get
        .settings
        .ok_or_else(|| Error::other("settings should contain a value".to_string()))?;
    assert_eq!(settings.tier, "db-f1-micro");

    println!("Testing list sql instances");
    let list = client
        .list(&project_id)
        .set_filter(format!("name:{name}"))
        .send()
        .await?;
    println!("SUCCESS on list sql instance: {list:?}");
    assert_eq!(list.items.len(), 1);
    assert!(list.items.into_iter().any(|v| v.name.eq(&name)));

    println!("Testing delete sql instance");
    let delete = client.delete(&project_id, &name).send().await?;
    println!("SUCCESS on delete sql instance: {delete:?}");
    assert_eq!(delete.target_id, name);

    Ok(())
}

const PREFIX: &str = "rust-sdk-testing-";
const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
pub const INSTANCE_NAME_LENGTH: usize = 98;
const INSTANCE_LABEL: &str = "rust-sdk-integration-test";

fn random_sql_instance_name(project_id: &str) -> String {
    let distr = RandomChars { chars: CHARSET };
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
        .map_err(Error::other)?;
    let stale_deadline = stale_deadline - Duration::from_secs(48 * 60 * 60);

    let stale_deadline = wkt::Timestamp::clamp(stale_deadline.as_secs() as i64, 0);

    let instances = client
        .list(project_id)
        .set_filter(format!(
            "name:{PREFIX}* AND settings.userLabels.{INSTANCE_LABEL}:true"
        ))
        .send()
        .await?;

    let pending_deletion = instances
        .items
        .into_iter()
        .filter_map(|instance| {
            if instance.create_time? < stale_deadline {
                Some(client.delete(project_id, instance.name).send())
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    futures::future::join_all(pending_deletion)
        .await
        .into_iter()
        .for_each(|res| {
            if let Err(err) = res {
                println!("Cleanup error: deleting sql instance resulted in {err:?}")
            }
        });

    Ok(())
}

pub async fn run_sql_tiers_service(
    builder: sql::builder::sql_tiers_service::ClientBuilder,
) -> Result<()> {
    // Enable a basic subscriber. Useful to troubleshoot problems and visually
    // verify tracing is doing something.
    #[cfg(feature = "log-integration-tests")]
    let _guard = {
        use tracing_subscriber::fmt::format::FmtSpan;
        let subscriber = tracing_subscriber::fmt()
            .with_level(true)
            .with_thread_ids(true)
            .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
            .finish();

        tracing::subscriber::set_default(subscriber)
    };

    let project_id = crate::project_id()?;
    let client: sql::client::SqlTiersService = builder.build().await?;

    let list = client.list(&project_id).send().await?;

    assert_ne!(
        list.items
            .into_iter()
            .find(|v| v.tier.eq("db-f1-micro"))
            .ok_or_else(|| Error::other("tiers list should contain db-f1-micro".to_string()))?
            .ram,
        0
    );

    Ok(())
}
