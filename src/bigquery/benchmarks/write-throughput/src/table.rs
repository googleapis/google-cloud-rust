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

use google_cloud_bigquery_v2::client::{DatasetService, TableService};
use google_cloud_bigquery_v2::model::get_table_request::TableMetadataView;
use google_cloud_bigquery_v2::model::{
    Dataset, DatasetReference, Table, TableFieldSchema, TableReference, TableSchema,
};
use rand::RngExt;
use rand::distr::Alphanumeric;
use std::time::Duration;

/// Manages dataset and table lifecycle for the write throughput benchmark.
pub struct BenchmarkEnvironment {
    pub project: String,
    pub dataset_id: String,
    pub table_ids: Vec<String>,
    is_temporary_dataset: bool,
    dataset_service: DatasetService,
}

impl BenchmarkEnvironment {
    /// Sets up the dataset (creating a temporary one if not specified) and tables,
    /// awaiting full table readiness before returning.
    pub async fn setup(project: &str, dataset_id: &str, num_tables: usize) -> anyhow::Result<Self> {
        let dataset_service = DatasetService::builder().build().await?;
        let table_service = TableService::builder().build().await?;

        let (dataset_id, is_temporary_dataset) = if dataset_id.is_empty() {
            let rand_suffix: String = rand::rng()
                .sample_iter(&Alphanumeric)
                .take(8)
                .map(char::from)
                .collect();
            (
                format!("rust_bq_bench_dataset_{}", rand_suffix.to_lowercase()),
                true,
            )
        } else {
            (dataset_id.to_string(), false)
        };

        if is_temporary_dataset {
            println!("# Creating temporary dataset: {}", dataset_id);
            dataset_service
                .insert_dataset()
                .set_project_id(project)
                .set_dataset(
                    Dataset::new()
                        .set_dataset_reference(DatasetReference::new().set_dataset_id(&dataset_id))
                        .set_labels([("bq_benchmark", "true")]),
                )
                .send()
                .await?;
        }

        let mut table_ids = Vec::with_capacity(num_tables);
        for t in 0..num_tables {
            let table_id = format!("table_{}", t);
            println!("# Creating table: {} in dataset: {}", table_id, dataset_id);

            create_table_and_await_readiness(&table_service, project, &dataset_id, &table_id)
                .await?;

            table_ids.push(table_id);
        }

        Ok(Self {
            project: project.to_string(),
            dataset_id,
            table_ids,
            is_temporary_dataset,
            dataset_service,
        })
    }

    /// Cleans up the temporary dataset and its tables if one was created.
    pub async fn cleanup(self) {
        if self.is_temporary_dataset {
            println!("# Cleaning up temporary dataset: {}", self.dataset_id);
            if let Err(e) = self
                .dataset_service
                .delete_dataset()
                .set_project_id(&self.project)
                .set_dataset_id(&self.dataset_id)
                .set_delete_contents(true)
                .send()
                .await
            {
                eprintln!("Error cleaning up dataset {}: {:?}", self.dataset_id, e);
            }
        }
    }
}

/// Creates a benchmark table and verifies readiness via GetTable RPC polling.
pub async fn create_table_and_await_readiness(
    table_service: &TableService,
    project: &str,
    dataset_id: &str,
    table_id: &str,
) -> anyhow::Result<()> {
    let schema = TableSchema::new().set_fields([TableFieldSchema::new()
        .set_name("payload")
        .set_type("STRING")]);

    table_service
        .insert_table()
        .set_project_id(project)
        .set_dataset_id(dataset_id)
        .set_table(
            Table::new()
                .set_table_reference(
                    TableReference::new()
                        .set_project_id(project)
                        .set_dataset_id(dataset_id)
                        .set_table_id(table_id),
                )
                .set_schema(schema),
        )
        .send()
        .await?;

    let mut attempts = 0;
    loop {
        match table_service
            .get_table()
            .set_project_id(project)
            .set_dataset_id(dataset_id)
            .set_table_id(table_id)
            .set_view(TableMetadataView::Full)
            .send()
            .await
        {
            Ok(_) => break,
            Err(_e) if attempts < 10 => {
                attempts += 1;
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Err(e) => return Err(e.into()),
        }
    }

    Ok(())
}
