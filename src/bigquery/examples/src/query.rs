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

mod batch;
mod clustered_table;
mod destination_table;
mod dry_run;
mod job_optional;
mod legacy;
mod legacy_large_results;
mod no_cache;
mod partitioned_table;
#[allow(clippy::module_inception)]
mod query;

use google_cloud_bigquery_v2::client::DatasetService;
use google_cloud_bigquery_v2::model::{Dataset, DatasetReference};
use google_cloud_test_utils::runtime_config::project_id;
use rand::{RngExt, distr::Alphanumeric};
use std::future::Future;
use std::pin::Pin;

fn random_id_suffix() -> String {
    rand::rng()
        .sample_iter(&Alphanumeric)
        .take(8)
        .map(char::from)
        .collect()
}

pub async fn run_samples() -> anyhow::Result<()> {
    let project_id = project_id()?;

    let pending: Vec<Pin<Box<dyn Future<Output = anyhow::Result<()>>>>> = vec![
        Box::pin(query::sample(&project_id)),
        Box::pin(no_cache::sample(&project_id)),
        Box::pin(batch::sample(&project_id)),
        Box::pin(dry_run::sample(&project_id)),
        Box::pin(legacy::sample(&project_id)),
        Box::pin(job_optional::sample(&project_id)),
        Box::pin(clustered_table::sample(&project_id)),
        Box::pin(partitioned_table::sample(&project_id)),
    ];
    let _ = futures::future::join_all(pending)
        .await
        .into_iter()
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(())
}

pub async fn run_samples_with_resources() -> anyhow::Result<()> {
    let project_id = project_id()?;
    let dataset_service = DatasetService::builder().build().await?;
    let dataset_id = format!("rust_bq_samples_{}", random_id_suffix());

    println!("Creating sample dataset `{dataset_id}`...");
    dataset_service
        .insert_dataset()
        .set_project_id(&project_id)
        .set_dataset(
            Dataset::new()
                .set_dataset_reference(DatasetReference::new().set_dataset_id(&dataset_id))
                .set_labels([("rust-sdk-integration-test", "true")]),
        )
        .send()
        .await?;

    let table_id_1 = format!("dest_{}", random_id_suffix());
    let table_id_2 = format!("dest_legacy_{}", random_id_suffix());

    let pending: Vec<Pin<Box<dyn Future<Output = anyhow::Result<()>>>>> = vec![
        Box::pin(destination_table::sample(
            &project_id,
            &dataset_id,
            &table_id_1,
        )),
        Box::pin(legacy_large_results::sample(
            &project_id,
            &dataset_id,
            &table_id_2,
        )),
    ];
    let res: anyhow::Result<Vec<_>> = futures::future::join_all(pending)
        .await
        .into_iter()
        .collect();

    println!("Deleting sample dataset `{dataset_id}`...");
    let _ = dataset_service
        .delete_dataset()
        .set_project_id(&project_id)
        .set_dataset_id(&dataset_id)
        .set_delete_contents(true)
        .send()
        .await;

    let _ = res?;
    Ok(())
}
