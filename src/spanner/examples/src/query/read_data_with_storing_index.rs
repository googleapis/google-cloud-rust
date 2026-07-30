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

// [START spanner_read_data_with_storing_index]
use google_cloud_spanner::client::DatabaseClient;
use google_cloud_spanner::key::KeySet;
use google_cloud_spanner::read::ReadRequest;

pub async fn sample(client: &DatabaseClient) -> anyhow::Result<()> {
    let read_request = ReadRequest::builder("Albums", ["AlbumId", "AlbumTitle", "MarketingBudget"])
        .with_index("AlbumsByAlbumTitle2", KeySet::all())
        .build();

    let transaction = client.single_use().build();
    let mut result_set = transaction.execute_read(read_request).await?;

    while let Some(row) = result_set.next().await.transpose()? {
        let album_id: i64 = row.get(0);
        let album_title: String = row.get(1);
        let marketing_budget: Option<i64> = row.get(2);

        match marketing_budget {
            Some(budget) => println!("{album_id} {album_title} {budget}"),
            None => println!("{album_id} {album_title} NULL"),
        }
    }

    Ok(())
}
// [END spanner_read_data_with_storing_index]
