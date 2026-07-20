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

// [START bigquery_query_dry_run]
use google_cloud_bigquery::client::BigQuery;

pub async fn sample(project_id: &str) -> anyhow::Result<()> {
    let client = BigQuery::builder().build().await?;

    let complete_query = client
        .query(
            "SELECT \
        name FROM `bigquery-public-data.usa_names.usa_1910_2013` \
        WHERE state = 'TX' \
        LIMIT 100",
        )
        .with_project_id(project_id)
        .set_dry_run(true)
        .set_location("US")
        .run()
        .await?
        .until_done()
        .await?;

    let bytes = complete_query.metadata().total_bytes_processed.unwrap_or(0);
    println!("This query will process {bytes} bytes.");
    Ok(())
}
// [END bigquery_query_dry_run]
