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

// [START bigquery_add_column_query_append]
use google_cloud_bigquery::client::BigQuery;
use google_cloud_bigquery::model::TableReference;

pub async fn sample(project_id: &str, dataset_id: &str, table_id: &str) -> anyhow::Result<()> {
    let client = BigQuery::builder().build().await?;

    // First, initialize the destination table with a single column
    let create_sql = format!(
        "CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.{table_id}` \
         (name STRING) AS \
         SELECT 'Alice' AS name;"
    );
    client
        .query(create_sql)
        .with_project_id(project_id)
        .set_location("US")
        .run()
        .await?
        .until_done()
        .await?;

    // Append rows using a query job that introduces a new column (`age`),
    // automatically updating the destination table schema.
    let append_sql = "SELECT 'Bob' AS name, 30 AS age;";
    let res = client
        .query(append_sql)
        .with_project_id(project_id)
        .set_destination_table(
            TableReference::new()
                .set_project_id(project_id)
                .set_dataset_id(dataset_id)
                .set_table_id(table_id),
        )
        .set_write_disposition("WRITE_APPEND")
        .set_schema_update_options(["ALLOW_FIELD_ADDITION"])
        .set_location("US")
        .run()
        .await?
        .until_done()
        .await?;

    println!("Appended rows with new column successfully: {:?}", res);
    Ok(())
}
// [END bigquery_add_column_query_append]
