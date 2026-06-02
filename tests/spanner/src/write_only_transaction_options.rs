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

use crate::client::get_emulator_host;
use google_cloud_spanner::client::DatabaseClient;
use google_cloud_spanner::model::CommitResponse;
use google_cloud_spanner::model::request_options::Priority;
use google_cloud_spanner::mutation::Mutation;
use google_cloud_spanner::statement::Statement;
use google_cloud_test_utils::resource_names::LowercaseAlphanumeric;
use google_cloud_wkt::Duration as WktDuration;

async fn run_write_only_options_test<F, Fut>(
    db_client: &DatabaseClient,
    id_prefix: &str,
    val: i64,
    execute_write: F,
) -> anyhow::Result<()>
where
    F: FnOnce(&DatabaseClient, Vec<Mutation>) -> Fut,
    Fut: std::future::Future<Output = google_cloud_spanner::Result<CommitResponse>>,
{
    let id = format!("{}-{}", id_prefix, LowercaseAlphanumeric.random_string(10));

    let mutation = Mutation::new_insert_builder("AllTypes")
        .set("Id")
        .to(&id)
        .set("ColInt64")
        .to(&val)
        .build();

    let result = execute_write(db_client, vec![mutation]).await?;

    if get_emulator_host().is_none() {
        let stats = result.commit_stats.expect(
            "Expected commit_stats to be returned when with_return_commit_stats is enabled in write_only_transaction",
        );

        assert_eq!(
            stats.mutation_count, 3,
            "Expected exactly 3 mutations (base row + secondary index updates) in commit stats"
        );
    }

    // Verify that the row was actually inserted using a separate read context
    let statement = Statement::builder("SELECT ColInt64 FROM AllTypes WHERE Id = @id")
        .add_param("id", &id)
        .build();
    let mut result_set = db_client
        .single_use()
        .build()
        .execute_query(statement)
        .await?;
    let row = result_set
        .next()
        .await
        .transpose()?
        .expect("Expected row to be inserted and readable");
    let value: i64 = row.get("ColInt64");
    assert_eq!(value, val, "Expected inserted value to match");

    Ok(())
}

pub async fn write_only_commit_configurations(db_client: &DatabaseClient) -> anyhow::Result<()> {
    run_write_only_options_test(db_client, "wo-conf", 101, |client, mutations| {
        client
            .write_only_transaction()
            .set_commit_priority(Priority::Low)
            .set_max_commit_delay(WktDuration::try_from("0.2s").expect("valid wkt duration"))
            .set_exclude_txn_from_change_streams(true)
            .set_return_commit_stats(true)
            .build()
            .write(mutations)
    })
    .await
}

pub async fn write_only_at_least_once_commit_configurations(
    db_client: &DatabaseClient,
) -> anyhow::Result<()> {
    run_write_only_options_test(db_client, "wo-least-once", 202, |client, mutations| {
        client
            .write_only_transaction()
            .set_commit_priority(Priority::Low)
            .set_max_commit_delay(WktDuration::try_from("0.2s").expect("valid wkt duration"))
            .set_exclude_txn_from_change_streams(true)
            .set_return_commit_stats(true)
            .build()
            .write_at_least_once(mutations)
    })
    .await
}
