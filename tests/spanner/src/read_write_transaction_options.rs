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

use crate::client::{get_database_id, get_emulator_host, get_real_spanner_config};
use google_cloud_spanner::Result as SpannerResult;
use google_cloud_spanner::client::{DatabaseClient, Spanner};
use google_cloud_spanner::model::request_options::Priority;
use google_cloud_spanner::{Mutation, Statement};
use google_cloud_test_utils::resource_names::LowercaseAlphanumeric;
use google_cloud_wkt::Duration as WktDuration;
use std::time::Duration;
use tokio::time::sleep;
use tracing::info;

async fn get_database_path() -> String {
    if get_emulator_host().is_some() {
        format!(
            "projects/{}/instances/{}/databases/{}",
            "test-project",
            "test-instance",
            get_database_id().await
        )
    } else {
        let (project, instance) =
            get_real_spanner_config().expect("Real Spanner config must be present if not emulator");
        format!(
            "projects/{}/instances/{}/databases/{}",
            project,
            instance,
            get_database_id().await
        )
    }
}

pub async fn runner_commit_configurations(db_client: &DatabaseClient) -> anyhow::Result<()> {
    let id = format!("adv-conf-{}", LowercaseAlphanumeric.random_string(10));

    let runner = db_client
        .read_write_transaction()
        .with_commit_priority(Priority::Low)
        .with_max_commit_delay(WktDuration::try_from("0.2s").expect("valid wkt duration"))
        .with_exclude_txn_from_change_streams(true)
        .with_return_commit_stats(true)
        .build()
        .await?;

    let result = runner
        .run(async |transaction| {
            let mutation = Mutation::new_insert_builder("AllTypes")
                .set("Id")
                .to(&id)
                .set("ColInt64")
                .to(&99_i64)
                .build();
            transaction.buffer(vec![mutation])?;
            Ok(())
        })
        .await?;

    if get_emulator_host().is_none() {
        let stats = result.commit_response.commit_stats.expect(
            "Expected commit_stats to be returned when with_return_commit_stats is enabled",
        );

        assert_eq!(
            stats.mutation_count, 3,
            "Expected exactly 3 mutations (base row + secondary index updates) in commit stats"
        );
    }

    Ok(())
}

pub async fn client_routing_success(_db_client: &DatabaseClient) -> anyhow::Result<()> {
    let database_path = get_database_path().await;

    // Build a new Spanner client using the builder to customize routing
    let spanner = Spanner::builder().build().await?;
    let custom_client = spanner
        .database_client(database_path)
        .with_leader_aware_routing(false)
        .build()
        .await?;

    let id = format!("adv-routing-{}", LowercaseAlphanumeric.random_string(10));
    let runner = custom_client.read_write_transaction().build().await?;

    runner
        .run(async |transaction| {
            let statement =
                Statement::builder("INSERT INTO AllTypes (Id, ColInt64) VALUES (@id, 42)")
                    .add_param("id", &id)
                    .build();
            transaction.execute_update(statement).await?;
            Ok(())
        })
        .await?;

    // Verify that the row was actually inserted
    let statement = Statement::builder("SELECT ColInt64 FROM AllTypes WHERE Id = @id")
        .add_param("id", &id)
        .build();
    let mut result_set = custom_client
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
    assert_eq!(value, 42, "Expected inserted value to be 42");

    Ok(())
}

pub async fn unauthorized_database_role_rejection(
    _db_client: &DatabaseClient,
) -> anyhow::Result<()> {
    if get_emulator_host().is_some() {
        info!(
            "Skipping unauthorized_database_role_rejection test on Spanner Emulator as it does not support FGAC."
        );
        return Ok(());
    }

    let database_path = get_database_path().await;

    // Build a new Spanner client using the builder to customize database role
    let spanner = Spanner::builder().build().await?;
    let custom_client_res = spanner
        .database_client(database_path)
        .with_database_role("invalid-unauthorized-role")
        .build()
        .await;

    assert!(
        custom_client_res.is_err(),
        "Expected client creation to fail with PermissionDenied due to unauthorized database role"
    );

    let err = custom_client_res.unwrap_err();
    let err_str = format!("{:?}", err);
    assert!(
        err_str.contains("PermissionDenied") || err_str.contains("Role not found"),
        "Expected PermissionDenied error, got: {}",
        err_str
    );

    Ok(())
}

pub async fn timeout_exceeded_transaction_abort(db_client: &DatabaseClient) -> anyhow::Result<()> {
    // Configure a tight transaction timeout (2ms)
    let runner = db_client
        .read_write_transaction()
        .with_transaction_timeout(Duration::from_millis(2))
        .build()
        .await?;

    let result: SpannerResult<()> = runner
        .run(async |transaction| {
            // Execute multiple statements with delay to guarantee timeout exceeding
            for _ in 0..5 {
                let statement = Statement::builder("SELECT * FROM AllTypes").build();
                let mut result_set = transaction.execute_query(statement).await?;
                while let Some(row) = result_set.next().await.transpose()? {
                    let _: String = row.get("Id");
                }
                sleep(Duration::from_millis(1)).await;
            }
            Ok(())
        })
        .await
        .map(|res| res.result);

    assert!(
        result.is_err(),
        "Expected transaction to fail with a DeadlineExceeded error"
    );

    let err = result.unwrap_err();
    let err_str = format!("{:?}", err);
    let err_str_lower = err_str.to_lowercase();
    assert!(
        err_str_lower.contains("deadlineexceeded")
            || err_str_lower.contains("timeout")
            || err_str_lower.contains("cancel"),
        "Expected DeadlineExceeded / timeout error, got: {}",
        err_str
    );

    Ok(())
}
