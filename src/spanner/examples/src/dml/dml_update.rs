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

// [START spanner_dml_getting_started_update]
use google_cloud_spanner::client::DatabaseClient;
use google_cloud_spanner::statement::Statement;

pub async fn sample(client: &DatabaseClient) -> anyhow::Result<()> {
    let runner = client.read_write_transaction().build().await?;

    runner
        .run(async |transaction| {
            // Transfer marketing budget from one album to another. We do it in a transaction to
            // ensure that the transfer is atomic.
            let select_statement1 = Statement::builder(
                r#"SELECT MarketingBudget
                 FROM Albums
                 WHERE SingerId = 2 AND AlbumId = 2"#,
            )
            .build();
            let mut result_set1 = transaction.execute_query(select_statement1).await?;
            let mut album2_budget = 0i64;
            if let Some(row) = result_set1.next().await.transpose()? {
                album2_budget = row.get("MarketingBudget");
            }

            let transfer = 200000i64;
            if album2_budget < transfer {
                return Ok(());
            }

            let select_statement2 = Statement::builder(
                r#"SELECT MarketingBudget
                 FROM Albums
                 WHERE SingerId = 1 AND AlbumId = 1"#,
            )
            .build();
            let mut result_set2 = transaction.execute_query(select_statement2).await?;
            let mut album1_budget = 0i64;
            if let Some(row) = result_set2.next().await.transpose()? {
                album1_budget = row.get("MarketingBudget");
            }

            album1_budget += transfer;
            album2_budget -= transfer;

            let update_statement1 = Statement::builder(
                r#"UPDATE Albums
                 SET MarketingBudget = @AlbumBudget
                 WHERE SingerId = 1 AND AlbumId = 1"#,
            )
            .add_param("AlbumBudget", &album1_budget)
            .build();
            transaction.execute_update(update_statement1).await?;

            let update_statement2 = Statement::builder(
                r#"UPDATE Albums
                 SET MarketingBudget = @AlbumBudget
                 WHERE SingerId = 2 AND AlbumId = 2"#,
            )
            .add_param("AlbumBudget", &album2_budget)
            .build();
            transaction.execute_update(update_statement2).await?;

            Ok(())
        })
        .await?;

    Ok(())
}
// [END spanner_dml_getting_started_update]
