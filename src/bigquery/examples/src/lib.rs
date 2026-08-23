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

pub mod job;
pub mod query;

use anyhow::Result;
use futures::stream::StreamExt;
use google_cloud_bigquery_v2::client::{DatasetService, TableService};
use google_cloud_bigquery_v2::model::{
    Dataset, DatasetReference, Table, TableReference, TableSchema,
};
use google_cloud_gax::error::rpc::Code;
use google_cloud_gax::paginator::ItemPaginator;
use rand::{RngExt, distr::Alphanumeric};

pub const INSTANCE_LABEL: &str = "rust-sdk-integration-test";

pub fn random_id_suffix() -> String {
    rand::rng()
        .sample_iter(&Alphanumeric)
        .take(8)
        .map(char::from)
        .collect()
}

pub fn random_dataset_id() -> String {
    let rand_suffix = random_id_suffix();
    format!("rust_bq_test_dataset_{rand_suffix}")
}

pub async fn create_dataset(
    client: &DatasetService,
    project_id: &str,
    dataset_id: &str,
) -> Result<Dataset> {
    println!("CREATING DATASET WITH ID: {dataset_id}");
    let ds = client
        .insert_dataset()
        .set_project_id(project_id)
        .set_dataset(
            Dataset::new()
                .set_dataset_reference(DatasetReference::new().set_dataset_id(dataset_id))
                .set_labels([(INSTANCE_LABEL, "true")]),
        )
        .send()
        .await?;
    Ok(ds)
}

pub async fn delete_dataset(
    client: &DatasetService,
    project_id: &str,
    dataset_id: &str,
) -> Result<()> {
    client
        .delete_dataset()
        .set_project_id(project_id)
        .set_dataset_id(dataset_id)
        .set_delete_contents(true)
        .send()
        .await?;
    Ok(())
}

pub async fn create_table(
    table_service: &TableService,
    project_id: &str,
    dataset_id: &str,
    table_id: &str,
    schema: TableSchema,
) -> Result<()> {
    println!("CREATING TABLE WITH ID: {table_id}");
    table_service
        .insert_table()
        .set_project_id(project_id)
        .set_dataset_id(dataset_id)
        .set_table(
            Table::new()
                .set_table_reference(
                    TableReference::new()
                        .set_project_id(project_id)
                        .set_dataset_id(dataset_id)
                        .set_table_id(table_id),
                )
                .set_schema(schema),
        )
        .send()
        .await?;

    Ok(())
}

pub async fn cleanup_stale_datasets(client: &DatasetService, project_id: &str) -> Result<()> {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    let stale_deadline = SystemTime::now().duration_since(UNIX_EPOCH)?;
    let stale_deadline = stale_deadline - Duration::from_secs(48 * 60 * 60);
    let stale_deadline = stale_deadline.as_millis() as i64;

    let list = client
        .list_datasets()
        .set_project_id(project_id)
        .set_filter(format!("labels.{INSTANCE_LABEL}"))
        .by_item()
        .into_stream();
    let datasets = list.collect::<Vec<_>>().await;

    let pending_all_datasets = datasets
        .iter()
        .filter_map(|v| match v {
            Ok(v) => {
                if let Some(dataset_id) = extract_dataset_id(project_id, &v.id) {
                    return Some(
                        client
                            .get_dataset()
                            .set_project_id(project_id)
                            .set_dataset_id(dataset_id)
                            .send(),
                    );
                }
                None
            }
            Err(_) => None,
        })
        .collect::<Vec<_>>();

    let stale_datasets = futures::future::join_all(pending_all_datasets)
        .await
        .into_iter()
        .filter_map(|r| match r {
            Ok(dataset) => Some(dataset),
            Err(e) if e.status().is_some_and(|s| s.code == Code::NotFound) => None,
            Err(_) => panic!("expected a successful get_dataset()"),
        })
        .filter_map(|dataset| {
            if dataset
                .labels
                .get(INSTANCE_LABEL)
                .is_some_and(|v| v == "true")
                && dataset.creation_time < stale_deadline
            {
                return Some(dataset);
            }
            None
        })
        .collect::<Vec<_>>();

    println!("found {} stale datasets", stale_datasets.len());

    let pending_deletion: Vec<_> = stale_datasets
        .into_iter()
        .filter_map(|ds| {
            if let Some(dataset_id) = extract_dataset_id(project_id, &ds.id) {
                return Some(
                    client
                        .delete_dataset()
                        .set_project_id(project_id)
                        .set_dataset_id(dataset_id)
                        .set_delete_contents(true)
                        .send(),
                );
            }
            None
        })
        .collect();

    futures::future::join_all(pending_deletion).await;

    Ok(())
}

fn extract_dataset_id(project_id: &str, id: &str) -> Option<String> {
    id.strip_prefix(project_id)
        .and_then(|s| s.strip_prefix(":"))
        .map(|v| v.to_string())
}
