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
use bigquery_samples::{
    cleanup_stale_datasets, create_dataset, create_table, delete_dataset, random_dataset_id,
};
use google_cloud_bigquery::client::BigQuery;
use google_cloud_bigquery_read::client::Read;
use google_cloud_bigquery_read::model::{DataFormat, ReadSession};
use google_cloud_bigquery_v2::client::{DatasetService, TableService};
use google_cloud_bigquery_v2::model::{TableFieldSchema, TableSchema};
use google_cloud_test_utils::runtime_config::project_id;

pub async fn run_reads() -> Result<()> {
    let project_id = project_id()?;
    let dataset_service = DatasetService::builder().with_tracing().build().await?;
    cleanup_stale_datasets(&dataset_service, &project_id).await?;

    let dataset_id = random_dataset_id();
    let _ = create_dataset(&dataset_service, &project_id, &dataset_id).await?;

    let table_service = TableService::builder().with_tracing().build().await?;
    let table_id = "reads";

    let result = async {
        let schema = TableSchema::new().set_fields([
            TableFieldSchema::new().set_name("name").set_type("STRING"),
            TableFieldSchema::new().set_name("age").set_type("INTEGER"),
        ]);
        create_table(&table_service, &project_id, &dataset_id, table_id, schema).await?;

        // Insert sample data into the table.
        let bq_client = BigQuery::builder().build().await?;
        let insert_query = format!(
            "INSERT INTO `{project_id}.{dataset_id}.{table_id}` (name, age) VALUES ('Alice', 25), ('Bob', 28), ('Charlie', 31)"
        );
        bq_client
            .query(insert_query)
            .with_project_id(&project_id)
            .set_labels(vec![(bigquery_samples::INSTANCE_LABEL, "true")])
            .until_done()
            .await?;

        read_rows(&project_id, &dataset_id, table_id).await?;
        Ok(())
    }
    .await;

    let _ = delete_dataset(&dataset_service, &project_id, &dataset_id).await;
    result
}

// Calls the server-streaming ReadRows RPC and consumes responses.
pub async fn read_rows(project_id: &str, dataset_id: &str, table_id: &str) -> Result<()> {
    let client = Read::builder().build().await?;

    let table = format!("projects/{project_id}/datasets/{dataset_id}/tables/{table_id}");
    let session = client
        .create_read_session()
        .set_parent(format!("projects/{project_id}"))
        .set_read_session(
            ReadSession::new()
                .set_data_format(DataFormat::Arrow)
                .set_table(table),
        )
        .set_max_stream_count(1)
        .send()
        .await?;

    assert!(
        !session.streams.is_empty(),
        "expected at least one stream in read session"
    );

    let stream_name = &session.streams[0].name;
    let mut stream = client
        .read_rows()
        .set_read_stream(stream_name)
        .send()
        .await?;

    let mut total_rows = 0;
    while let Some(response) = stream.next().await {
        let response = response?;
        total_rows += response.row_count;
    }
    assert_eq!(total_rows, 3, "expected 3 rows to be read from stream");

    Ok(())
}
