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
mod browse_table;
mod clustered_table;
mod ddl_create_routine;
mod ddl_create_view;
mod destination_table;
mod dml_update;
mod dry_run;
mod job_optional;
mod label_job;
mod legacy;
mod legacy_large_results;
mod no_cache;
mod pagination;
mod params_arrays;
mod params_named;
mod params_named_types;
mod params_positional;
mod params_positional_types;
mod params_structs;
mod params_timestamps;
mod partitioned_table;
#[allow(clippy::module_inception)]
mod query;
mod query_append;
mod script;
mod total_rows;

use google_cloud_bigquery_v2::client::DatasetService;
use google_cloud_bigquery_v2::model::{Dataset, DatasetReference};
use google_cloud_test_utils::runtime_config::project_id;
use rand::{RngExt, distr::Alphanumeric};
use std::future::Future;
use std::pin::Pin;

const INSTANCE_LABEL: &str = "rust-sdk-integration-test";

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
        Box::pin(params_positional::sample(&project_id)),
        Box::pin(params_named::sample(&project_id)),
        Box::pin(params_arrays::sample(&project_id)),
        Box::pin(params_timestamps::sample(&project_id)),
        Box::pin(clustered_table::sample(&project_id)),
        Box::pin(partitioned_table::sample(&project_id)),
        Box::pin(params_structs::sample(&project_id)),
        Box::pin(params_named_types::sample(&project_id)),
        Box::pin(params_positional_types::sample(&project_id)),
        Box::pin(browse_table::sample(&project_id)),
        Box::pin(pagination::sample(&project_id)),
        Box::pin(total_rows::sample(&project_id)),
        Box::pin(label_job::sample(&project_id)),
        Box::pin(script::sample(&project_id)),
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
                .set_labels([(INSTANCE_LABEL, "true")]),
        )
        .send()
        .await?;

    let destination_table_id = format!("dest_{}", random_id_suffix());
    let dest_legacy_table_id = format!("dest_legacy_{}", random_id_suffix());
    let dml_table_id = format!("dml_{}", random_id_suffix());
    let ddl_view_id = format!("view_{}", random_id_suffix());
    let ddl_routine_id = format!("fn_{}", random_id_suffix());
    let append_table_id = format!("append_{}", random_id_suffix());

    let pending: Vec<Pin<Box<dyn Future<Output = anyhow::Result<()>>>>> = vec![
        Box::pin(destination_table::sample(
            &project_id,
            &dataset_id,
            &destination_table_id,
        )),
        Box::pin(legacy_large_results::sample(
            &project_id,
            &dataset_id,
            &dest_legacy_table_id,
        )),
        Box::pin(dml_update::sample(&project_id, &dataset_id, &dml_table_id)),
        Box::pin(ddl_create_view::sample(
            &project_id,
            &dataset_id,
            &ddl_view_id,
        )),
        Box::pin(ddl_create_routine::sample(
            &project_id,
            &dataset_id,
            &ddl_routine_id,
        )),
        Box::pin(query_append::sample(
            &project_id,
            &dataset_id,
            &append_table_id,
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

// Validates resource identifier as a valid BigQuery resource name.
fn validate_resource_name(name: &str) -> anyhow::Result<()> {
    if name.is_empty()
        || !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
    {
        anyhow::bail!("Invalid resource identifier: {name}");
    }
    Ok(())
}

// Validates resource identifiers before constructing SQL strings to prevent SQL injection.
pub(crate) fn validate_resource_names(
    project_id: &str,
    dataset_id: &str,
    resource_id: &str, // table, routine or view
) -> anyhow::Result<()> {
    validate_resource_name(project_id)?;
    validate_resource_name(dataset_id)?;
    validate_resource_name(resource_id)?;

    Ok(())
}
