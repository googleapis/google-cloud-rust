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

mod arrow;

use super::INSTANCE_LABEL;
use crate::dataset::{cleanup_stale_datasets, create_dataset, delete_dataset, random_dataset_id};
use crate::query::UserRecord;
use anyhow::Result;
use google_cloud_bigquery::client::BigQuery;
use google_cloud_bigquery_v2::client::{DatasetService, TableService};
use google_cloud_bigquery_v2::model::{Table, TableFieldSchema, TableReference, TableSchema};
use google_cloud_test_utils::runtime_config::project_id;

pub async fn run_writes() -> Result<()> {
    let project_id = project_id()?;
    let dataset_service = DatasetService::builder().with_tracing().build().await?;
    cleanup_stale_datasets(&dataset_service, &project_id).await?;

    let dataset_id = random_dataset_id();
    let _ = create_dataset(&dataset_service, &project_id, &dataset_id).await?;

    let table_service = TableService::builder().with_tracing().build().await?;
    let table_id = "writes";

    let result = async {
        create_table(&table_service, &project_id, &dataset_id, table_id).await?;
        arrow::basic(&project_id, &dataset_id, table_id).await
    }
    .await;

    let _ = delete_dataset(&dataset_service, &project_id, &dataset_id).await;
    result
}

async fn create_table(
    table_service: &TableService,
    project_id: &str,
    dataset_id: &str,
    table_id: &str,
) -> Result<()> {
    println!("CREATING TABLE WITH ID: {table_id}");
    let schema = TableSchema::new().set_fields([
        TableFieldSchema::new().set_name("name").set_type("STRING"),
        TableFieldSchema::new().set_name("age").set_type("INTEGER"),
    ]);
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

async fn read_table(project_id: &str, dataset_id: &str, table_id: &str) -> Result<Vec<UserRecord>> {
    let client = BigQuery::builder().build().await?;
    let query = format!("SELECT * FROM `{project_id}.{dataset_id}.{table_id}` ORDER BY name");
    let mut rows = client
        .query(query)
        .with_project_id(project_id)
        .set_labels(vec![(INSTANCE_LABEL, "true")])
        .until_done()
        .await?
        .read();

    let mut users = Vec::new();
    while let Some(row) = rows.next().await {
        users.push(row?.try_into()?);
    }
    Ok(users)
}
