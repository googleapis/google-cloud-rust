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
mod flaky;

use anyhow::Result;
use bigquery_samples::{
    cleanup_stale_datasets, create_dataset, create_table, delete_dataset, random_dataset_id,
};
use google_cloud_bigquery::client::{BigQuery, Write};
use google_cloud_bigquery::query::FromRow;
use google_cloud_bigquery_v2::client::{DatasetService, TableService};
use google_cloud_bigquery_v2::model::{Dataset, DatasetReference, TableFieldSchema, TableSchema};
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
        let schema = TableSchema::new().set_fields([
            TableFieldSchema::new().set_name("name").set_type("STRING"),
            TableFieldSchema::new().set_name("age").set_type("INTEGER"),
            TableFieldSchema::new().set_name("test").set_type("STRING"),
        ]);
        create_table(
            &table_service,
            &project_id,
            &dataset_id,
            table_id,
            schema.clone(),
        )
        .await?;
        let client = Write::builder().build().await?;
        arrow::basic(&client, &project_id, &dataset_id, table_id).await?;
        arrow::pending(&client, &project_id, &dataset_id, table_id).await?;
        arrow::committed(&client, &project_id, &dataset_id, table_id).await?;
        arrow::buffered(&client, &project_id, &dataset_id, table_id).await?;
        arrow::attach(&client, &project_id, &dataset_id, table_id).await?;

        create_table(
            &table_service,
            &project_id,
            &dataset_id,
            "multi1",
            schema.clone(),
        )
        .await?;
        create_table(&table_service, &project_id, &dataset_id, "multi2", schema).await?;
        arrow::multiplex(&client, &project_id, &dataset_id, "multi").await?;

        Ok(())
    }
    .await;

    let _ = delete_dataset(&dataset_service, &project_id, &dataset_id).await;
    result
}

pub async fn run_writes_flaky() -> Result<()> {
    let project_id = project_id()?;
    let dataset_service = DatasetService::builder().with_tracing().build().await?;
    cleanup_stale_datasets(&dataset_service, &project_id).await?;

    let dataset_id = format!("rust_bq_flaky_{}", bigquery_samples::random_id_suffix());
    let _ = dataset_service
        .insert_dataset()
        .set_project_id(&project_id)
        .set_dataset(
            Dataset::new()
                .set_dataset_reference(DatasetReference::new().set_dataset_id(&dataset_id))
                .set_location(flaky::FLAKY_REGION)
                .set_labels([(bigquery_samples::INSTANCE_LABEL, "true")]),
        )
        .send()
        .await?;

    let table_service = TableService::builder().with_tracing().build().await?;
    let schema = TableSchema::new().set_fields([
        TableFieldSchema::new().set_name("name").set_type("STRING"),
        TableFieldSchema::new().set_name("age").set_type("INTEGER"),
        TableFieldSchema::new().set_name("test").set_type("STRING"),
    ]);

    let result = async {
        let client = Write::builder().build().await?;
        flaky::reconnect_on_close_sequential(
            &client,
            &table_service,
            &project_id,
            &dataset_id,
            schema.clone(),
        )
        .await?;
        flaky::reconnect_on_close_parallel(
            &client,
            &table_service,
            &project_id,
            &dataset_id,
            schema.clone(),
        )
        .await?;
        flaky::initial_connect_failure_sequential(
            &client,
            &table_service,
            &project_id,
            &dataset_id,
            schema.clone(),
        )
        .await?;
        flaky::initial_connect_failure_parallel(
            &client,
            &table_service,
            &project_id,
            &dataset_id,
            schema.clone(),
        )
        .await?;
        flaky::reconnect_on_close_default(
            &client,
            &table_service,
            &project_id,
            &dataset_id,
            schema,
        )
        .await?;

        Ok(())
    }
    .await;

    let _ = delete_dataset(&dataset_service, &project_id, &dataset_id).await;
    result
}

#[derive(FromRow, Debug, PartialEq)]
pub(crate) struct WriteUserRecord {
    pub(crate) name: String,
    pub(crate) age: i64,
    pub(crate) test: String,
}

#[derive(FromRow, Debug, PartialEq)]
pub(crate) struct WriteCountRecord {
    pub(crate) count: i64,
}

pub(crate) async fn read_writes_table(
    project_id: &str,
    dataset_id: &str,
    table_id: &str,
    test_filter: &str,
) -> Result<Vec<WriteUserRecord>> {
    let client = BigQuery::builder().build().await?;
    let query = format!(
        "SELECT * FROM `{project_id}.{dataset_id}.{table_id}` WHERE test = '{test_filter}' ORDER BY name"
    );
    let mut rows = client
        .query(query)
        .with_project_id(project_id)
        .set_labels(vec![(bigquery_samples::INSTANCE_LABEL, "true")])
        .until_done()
        .await?
        .read();

    let mut users = Vec::new();
    while let Some(row) = rows.next().await {
        users.push(row?.try_into()?);
    }
    Ok(users)
}

pub(crate) async fn count_writes_table(
    project_id: &str,
    dataset_id: &str,
    table_id: &str,
    test_filter: &str,
    location: Option<&str>,
) -> Result<i64> {
    let client = BigQuery::builder().build().await?;
    let query = format!(
        "SELECT COUNT(*) as count FROM `{project_id}.{dataset_id}.{table_id}` WHERE test = '{test_filter}'"
    );
    let mut builder = client
        .query(query)
        .with_project_id(project_id)
        .set_labels(vec![(bigquery_samples::INSTANCE_LABEL, "true")]);
    if let Some(loc) = location {
        builder = builder.set_location(loc);
    }
    let mut rows = builder.until_done().await?.read();

    if let Some(row) = rows.next().await {
        let count_row: WriteCountRecord = row?.try_into()?;
        return Ok(count_row.count);
    }
    Ok(0)
}
