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

use anyhow::Result;
use google_cloud_bigtable_admin_v2::client::BigtableTableAdmin;
use google_cloud_bigtable_admin_v2::model::{ColumnFamily, Table};
use google_cloud_gax::exponential_backoff::ExponentialBackoffBuilder;
use google_cloud_gax::options::RequestOptionsBuilder as _;
use google_cloud_gax::polling_error_policy::{Aip194Strict, PollingErrorPolicyExt as _};
use std::time::Duration;

pub async fn check_consistency(
    table_admin: &BigtableTableAdmin,
    instance_name: &str,
) -> Result<()> {
    tracing::info!("creating table in {instance_name}");
    let table = table_admin
        .create_table()
        .set_parent(instance_name)
        .set_table_id("test-table")
        .set_table(Table::new().set_column_families([("cf1", ColumnFamily::new())]))
        .send()
        .await?;
    tracing::info!("created table: {}", table.name);

    let got = table_admin.get_table().set_name(&table.name).send().await?;
    assert_eq!(got.name, table.name);

    tracing::info!("generating consistency token for {}", table.name);
    let token = table_admin
        .generate_consistency_token()
        .set_name(&table.name)
        .send()
        .await?;
    tracing::info!(
        "waiting for consistency with token {}",
        token.consistency_token
    );
    let backoff = ExponentialBackoffBuilder::new()
        .with_initial_delay(Duration::from_millis(500))
        .with_maximum_delay(Duration::from_secs(5))
        .clamp();
    table_admin
        .check_consistency()
        .set_name(&table.name)
        .set_consistency_token(&token.consistency_token)
        .with_polling_error_policy(Aip194Strict.with_time_limit(Duration::from_secs(180)))
        .with_polling_backoff_policy(backoff)
        .until_consistent()
        .await?;
    tracing::info!("table {} is consistent", table.name);

    tracing::info!("deleting table: {}", table.name);
    table_admin
        .delete_table()
        .set_name(&table.name)
        .send()
        .await?;

    Ok(())
}
