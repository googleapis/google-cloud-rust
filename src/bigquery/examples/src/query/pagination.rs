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

// [START bigquery_query_pagination]
use google_cloud_bigquery::client::BigQuery;

pub async fn sample(project_id: &str) -> anyhow::Result<()> {
    let client = BigQuery::builder().build().await?;

    let mut rows = client
        .query(
            "SELECT \
        name FROM `bigquery-public-data.usa_names.usa_1910_2013` \
        WHERE state = 'TX' \
        LIMIT 2500",
        )
        .with_project_id(project_id)
        .set_max_results(1000_u32)
        .set_location("US")
        .run()
        .await?
        .until_done()
        .await?
        .read()
        .set_max_rows_buffered(1000);

    let mut count = 0;
    while let Some(row) = rows.next().await.transpose()? {
        let _name: String = row.get("name");
        count += 1;
    }
    println!("Total rows fetched via pagination: {count}");
    Ok(())
}
// [END bigquery_query_pagination]
