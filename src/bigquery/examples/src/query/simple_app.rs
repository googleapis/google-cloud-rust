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

// [START bigquery_simple_app_all]
// [START bigquery_simple_app_deps]
use google_cloud_bigquery::FromRow;
use google_cloud_bigquery::client::BigQuery;
// [END bigquery_simple_app_deps]

pub async fn sample(project_id: &str) -> anyhow::Result<()> {
    // [START bigquery_simple_app_client]
    let client = BigQuery::builder().build().await?;
    // [END bigquery_simple_app_client]

    // [START bigquery_simple_app_query]
    let query = r#"
SELECT
    CONCAT('https://stackoverflow.com/questions/', CAST(id as STRING)) as url,
    view_count
FROM `bigquery-public-data.stackoverflow.posts_questions`
WHERE tags like '%google-bigquery%'
ORDER BY view_count DESC
LIMIT 10;
"#;
    let mut rows = client
        .query(query)
        .with_project_id(project_id)
        .run()
        .await?
        .until_done()
        .await?
        .read();
    // [END bigquery_simple_app_query]

    // [START bigquery_simple_app_print]
    #[derive(FromRow, Debug)]
    struct StackOverflowRow {
        url: String,
        view_count: i64,
    }

    while let Some(row) = rows.next().await.transpose()? {
        let row: StackOverflowRow = row.try_into()?;
        println!("url: {} views: {}", row.url, row.view_count);
    }
    // [END bigquery_simple_app_print]

    Ok(())
}
// [END bigquery_simple_app_all]
