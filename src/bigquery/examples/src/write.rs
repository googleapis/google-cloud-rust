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

mod arrow_default;
mod json_default;

use crate::{create_dataset, create_table, delete_dataset, random_dataset_id};
use google_cloud_bigquery_v2::client::{DatasetService, TableService};
use google_cloud_bigquery_v2::model::{TableFieldSchema, TableSchema};
use google_cloud_test_utils::runtime_config::project_id;

pub async fn run_samples() -> anyhow::Result<()> {
    let project_id = project_id()?;

    let dataset_service = DatasetService::builder().with_tracing().build().await?;
    let dataset_id = random_dataset_id();
    let _ = create_dataset(&dataset_service, &project_id, &dataset_id).await?;

    let table_service = TableService::builder().with_tracing().build().await?;
    let table_id = "samples";

    let schema = TableSchema::new().set_fields([
        TableFieldSchema::new()
            .set_name("string")
            .set_type("STRING"),
        TableFieldSchema::new().set_name("int").set_type("INTEGER"),
    ]);
    create_table(&table_service, &project_id, &dataset_id, table_id, schema).await?;

    let result = async {
        arrow_default::sample(&project_id, &dataset_id, table_id).await?;
        json_default::sample(&project_id, &dataset_id, table_id).await?;
        Ok(())
    }
    .await;

    let _ = delete_dataset(&dataset_service, &project_id, &dataset_id).await;
    result
}
