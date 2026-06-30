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

// [START spanner_query_data_with_new_column]
use google_cloud_spanner::client::DatabaseClient;
use google_cloud_spanner::statement::Statement;

pub async fn sample(client: &DatabaseClient) -> anyhow::Result<()> {
    let statement =
        Statement::builder("SELECT SingerId, AlbumId, MarketingBudget FROM Albums").build();
    let transaction = client.single_use().build();
    let mut result_set = transaction.execute_query(statement).await?;

    while let Some(row) = result_set.next().await.transpose()? {
        let singer_id: i64 = row.get("SingerId");
        let album_id: i64 = row.get("AlbumId");
        let marketing_budget: Option<i64> = row.get("MarketingBudget");

        match marketing_budget {
            Some(budget) => println!("{singer_id} {album_id} {budget}"),
            None => println!("{singer_id} {album_id} NULL"),
        }
    }
    Ok(())
}
// [END spanner_query_data_with_new_column]
