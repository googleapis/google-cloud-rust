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

// [START bigquery_create_routine_ddl]
use google_cloud_bigquery::client::BigQuery;

pub async fn sample(project_id: &str, dataset_id: &str, routine_id: &str) -> anyhow::Result<()> {
    let client = BigQuery::builder().build().await?;

    // Create a SQL UDF routine using standard SQL DDL
    let ddl_sql = format!(
        "CREATE OR REPLACE FUNCTION `{project_id}.{dataset_id}.{routine_id}`(x INT64, y INT64) \
         RETURNS INT64 \
         AS (x * y);"
    );

    client
        .query(&ddl_sql)
        .with_project_id(project_id)
        .set_location("US")
        .run()
        .await?
        .until_done()
        .await?;

    println!("Routine `{dataset_id}.{routine_id}` created successfully using DDL.");
    Ok(())
}
// [END bigquery_create_routine_ddl]
