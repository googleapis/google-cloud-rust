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

// [START bigquery_update_with_dml]
use google_cloud_bigquery::client::BigQuery;

pub async fn sample(project_id: &str, dataset_id: &str, table_id: &str) -> anyhow::Result<()> {
    let client = BigQuery::builder().build().await?;

    // First, create a sample table with initial data
    let create_sql = format!(
        "CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.{table_id}` \
         (name STRING, quantity INT64) AS \
         SELECT 'Item A' AS name, 10 AS quantity UNION ALL \
         SELECT 'Item B' AS name, 20 AS quantity;"
    );
    client
        .query(create_sql)
        .with_project_id(project_id)
        .set_location("US")
        .run()
        .await?
        .until_done()
        .await?;

    // Execute a DML UPDATE query to modify existing rows in the table
    let dml_sql = format!(
        "UPDATE `{project_id}.{dataset_id}.{table_id}` \
         SET quantity = quantity + 5 \
         WHERE name = 'Item A';"
    );
    let res = client
        .query(dml_sql)
        .with_project_id(project_id)
        .set_location("US")
        .run()
        .await?
        .until_done()
        .await?;

    println!("DML UPDATE job completed successfully: {:?}", res);
    Ok(())
}
// [END bigquery_update_with_dml]
