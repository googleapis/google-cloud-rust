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

// [START spanner_init_client]
use google_cloud_spanner::client::{DatabaseClient, Spanner};
use google_cloud_spanner_admin_database_v1::client::DatabaseAdmin;

pub async fn sample(database_name: &str) -> anyhow::Result<(DatabaseClient, DatabaseAdmin)> {
    // The builder automatically handles SPANNER_EMULATOR_HOST if set in the environment.
    let client = Spanner::builder().build().await?;

    // Creates a client that can be used to execute queries and transactions on a specific database.
    let database_client = client.database_client(database_name).build().await?;

    // Creates a client that can be used to execute DDL and other administrative operations on databases.
    let admin_client = client.database_admin_builder().build().await?;

    Ok((database_client, admin_client))
}
// [END spanner_init_client]
