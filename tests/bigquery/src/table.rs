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
use google_cloud_bigquery_v2::client::TableService;
use google_cloud_bigquery_v2::model::{Table, TableFieldSchema, TableReference, TableSchema};

pub(crate) async fn create_table(
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
