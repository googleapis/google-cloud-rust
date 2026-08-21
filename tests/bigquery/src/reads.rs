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

use crate::dataset::{cleanup_stale_datasets, create_dataset, delete_dataset, random_dataset_id};
use crate::table::create_table;
use anyhow::Result;
use google_cloud_bigquery_read::client::Read;
use google_cloud_bigquery_read::model::{DataFormat, ReadSession};
use google_cloud_bigquery_v2::client::{DatasetService, TableService};
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
        create_table(&table_service, &project_id, &dataset_id, table_id).await?;
        basic(&project_id, &dataset_id, table_id).await
    }
    .await;

    let _ = delete_dataset(&dataset_service, &project_id, &dataset_id).await;
    result
}

// Calls the one unary RPC the service has to offer.
pub async fn basic(project_id: &str, dataset_id: &str, table_id: &str) -> Result<()> {
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
    println!("Successfully created ReadSession: {session:?}");
    Ok(())
}
