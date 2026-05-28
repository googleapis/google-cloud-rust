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

// [START spanner_update_data]
use google_cloud_spanner::client::{DatabaseClient, Mutation};

pub async fn sample(client: &DatabaseClient) -> anyhow::Result<()> {
    let mutations = vec![
        Mutation::new_update_builder("Albums")
            .set("SingerId")
            .to(&1)
            .set("AlbumId")
            .to(&1)
            .set("MarketingBudget")
            .to(&100000)
            .build(),
        Mutation::new_update_builder("Albums")
            .set("SingerId")
            .to(&2)
            .set("AlbumId")
            .to(&2)
            .set("MarketingBudget")
            .to(&500000)
            .build(),
    ];

    println!("Updating MarketingBudget on Albums...");
    let write_transaction = client.write_only_transaction().build();
    write_transaction.write(mutations).await?;
    println!("Updated budget successfully.");

    Ok(())
}
// [END spanner_update_data]
