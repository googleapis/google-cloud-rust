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
use gax::error::Error;
use rand::{Rng, distr::Alphanumeric};

pub async fn dataset_admin(
    builder: bigquery::builder::dataset_service::ClientBuilder,
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
    let client = builder.build().await?;
    cleanup_stale_datasets(&client, &project_id).await?;

    let rand_suffix: String = rand::rng()
        .sample_iter(&Alphanumeric)
        .take(8)
        .map(char::from)
        .collect();

    let ds_name = format!("rust_bq_test_dataset_{rand_suffix}");

    println!("CREATING DATASET WITH ID: {ds_name}");

    let create = client
        .insert_dataset(&project_id)
        .set_dataset(bigquery::model::Dataset::new().set_dataset_reference(
            bigquery::model::DatasetReference::new().set_dataset_id(&ds_name),
        ))
        .send()
        .await?;
    println!("CREATE DATASET = {create:?}");

    assert!(create.dataset_reference.is_some());

    let list = client.list_datasets(&project_id).send().await?;
    println!("LIST DATASET = {} entries", list.datasets.len());

    assert!(!list.datasets.is_empty());
    assert!(list.datasets.len() > 1);
    assert!(list.datasets.iter().any(|v| v.id.contains(&ds_name)));

    client
        .delete_dataset(&project_id, &ds_name)
        .set_delete_contents(true)
        .send()
        .await?;
    println!("DELETE DATASET");

    Ok(())
}

async fn cleanup_stale_datasets(
    client: &bigquery::client::DatasetService,
    project_id: &str,
) -> Result<()> {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    let stale_deadline = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(Error::other)?;
    let stale_deadline = stale_deadline - Duration::from_secs(48 * 60 * 60);
    let stale_deadline = stale_deadline.as_millis() as i64;

    let list = client.list_datasets(project_id).send().await?;
    let pending_all_datasets = list
        .datasets
        .iter()
        .map(|v| {
            client
                .get_dataset(
                    project_id,
                    v.dataset_reference.as_ref().map_or("", |v| &v.dataset_id),
                )
                .send()
        })
        .collect::<Vec<_>>();

    let stale_datasets = futures::future::join_all(pending_all_datasets)
        .await
        .into_iter()
        .filter_map(|r| {
            if r.as_ref()
                .is_ok_and(|ds| ds.creation_time < stale_deadline && ds.id.contains("bq_rust"))
            {
                return r.ok();
            }
            None
        })
        .collect::<Vec<_>>();

    println!("found {} stale datasets", stale_datasets.len());

    let pending_deletion: Vec<_> = stale_datasets
        .iter()
        .map(|ds| {
            client
                .delete_dataset(
                    project_id,
                    ds.dataset_reference.as_ref().map_or("", |v| &v.dataset_id),
                )
                .set_delete_contents(true)
                .send()
        })
        .collect();

    futures::future::join_all(pending_deletion)
        .await
        .into_iter()
        .zip(stale_datasets)
        .for_each(|(r, ds)| println!("{} = {r:?}", ds.id));

    Ok(())
}
