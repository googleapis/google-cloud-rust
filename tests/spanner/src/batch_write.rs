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
use google_cloud_gax::error::rpc::Code;
use google_cloud_spanner::client::DatabaseClient;
use google_cloud_spanner::model::BatchWriteResponse;
use google_cloud_spanner::mutation::{Mutation, MutationGroup};
use google_cloud_spanner::statement::Statement;
use google_cloud_test_utils::resource_names::LowercaseAlphanumeric;
use std::time::Duration;
use tokio::time::sleep;

pub async fn batch_write(db_client: &DatabaseClient) -> Result<()> {
    let id1 = format!("batch-write1-{}", LowercaseAlphanumeric.random_string(10));
    let id2 = format!("batch-write2-{}", LowercaseAlphanumeric.random_string(10));

    let m1 = Mutation::new_insert_or_update_builder("AllTypes")
        .set("Id")
        .to(&id1)
        .set("ColString")
        .to(&"batch-write-1")
        .build();

    let m2 = Mutation::new_insert_or_update_builder("AllTypes")
        .set("Id")
        .to(&id2)
        .set("ColString")
        .to(&"batch-write-2")
        .build();

    let group1 = MutationGroup::new(vec![m1]);
    let group2 = MutationGroup::new(vec![m2]);

    let mut attempts = 0;
    const MAX_ATTEMPTS: u32 = 500;

    let mut seen_indexes = Vec::new();
    loop {
        attempts += 1;
        let transaction = db_client.batch_write_transaction().build();
        let mut stream = match transaction
            .execute_streaming(vec![group1.clone(), group2.clone()])
            .await
        {
            Ok(s) => s,
            Err(e) if e.status().map(|s| s.code) == Some(Code::Aborted) => {
                if attempts >= MAX_ATTEMPTS {
                    anyhow::bail!(
                        "BatchWrite failed after {} attempts due to Aborted",
                        attempts
                    );
                }
                sleep(Duration::from_millis(rand::random_range(10_u64..=50_u64))).await;
                continue;
            }
            Err(e) => return Err(e.into()),
        };

        seen_indexes.clear();
        let mut aborted = false;
        while let Some(response) = stream.next().await {
            match response {
                Ok(resp) => {
                    if let Some(status) = &resp.status {
                        if status.code == Code::Aborted as i32 {
                            aborted = true;
                            break;
                        }
                        assert_eq!(
                            status.code,
                            Code::Ok as i32,
                            "BatchWriteResponse status was not OK: {}",
                            status.message
                        );
                    }
                    seen_indexes.extend(resp.indexes);
                }
                Err(e) if e.status().map(|s| s.code) == Some(Code::Aborted) => {
                    aborted = true;
                    break;
                }
                Err(e) => return Err(e.into()),
            }
        }

        if aborted {
            if attempts >= MAX_ATTEMPTS {
                anyhow::bail!(
                    "BatchWrite failed after {} attempts due to Aborted in stream",
                    attempts
                );
            }
            sleep(Duration::from_millis(rand::random_range(10_u64..=50_u64))).await;
            continue;
        }

        break;
    }

    // Verify that all groups were applied.
    assert!(seen_indexes.contains(&0));
    assert!(seen_indexes.contains(&1));

    // Read back to verify
    let read_tx = db_client.single_use().build();
    let stmt =
        Statement::builder("SELECT ColString FROM AllTypes WHERE Id IN (@id1, @id2) ORDER BY Id")
            .add_param("id1", &id1)
            .add_param("id2", &id2)
            .build();
    let mut rs = read_tx.execute_query(stmt).await?;

    let mut rows = Vec::new();
    while let Some(row) = rs.next().await {
        rows.push(row?);
    }
    assert_eq!(rows.len(), 2, "Expected precisely 2 rows inserted/updated");
    assert_eq!(rows[0].get::<String, _>("ColString"), "batch-write-1");
    assert_eq!(rows[1].get::<String, _>("ColString"), "batch-write-2");

    Ok(())
}

pub async fn batch_write_partial_failure(db_client: &DatabaseClient) -> Result<()> {
    let run_id = LowercaseAlphanumeric.random_string(10);
    let id_ok = format!("batch-partial-ok-{}", run_id);
    let id_err = format!("batch-partial-err-{}", run_id);

    // Group 1: Valid mutation to "AllTypes" table
    let mutation_ok = Mutation::new_insert_or_update_builder("AllTypes")
        .set("Id")
        .to(&id_ok)
        .set("ColString")
        .to(&"batch-write-ok")
        .build();

    // Group 2: Invalid mutation targeting a non-existent table "NonExistentTable"
    let mutation_err = Mutation::new_insert_or_update_builder("NonExistentTable")
        .set("Id")
        .to(&id_err)
        .build();

    let group1 = MutationGroup::new(vec![mutation_ok]);
    let group2 = MutationGroup::new(vec![mutation_err]);

    let mut attempts = 0;
    const MAX_ATTEMPTS: u32 = 500;

    let mut seen_ok;
    let mut seen_err;

    loop {
        attempts += 1;
        let transaction = db_client.batch_write_transaction().build();
        let mut stream = match transaction
            .execute_streaming(vec![group1.clone(), group2.clone()])
            .await
        {
            Ok(s) => s,
            Err(e) if e.status().map(|s| s.code) == Some(Code::Aborted) => {
                if attempts >= MAX_ATTEMPTS {
                    anyhow::bail!(
                        "BatchWrite failed after {} attempts due to Aborted",
                        attempts
                    );
                }
                sleep(Duration::from_millis(rand::random_range(10_u64..=50_u64))).await;
                continue;
            }
            Err(e) => return Err(e.into()),
        };

        seen_ok = false;
        seen_err = false;
        let mut aborted = false;

        while let Some(response) = stream.next().await {
            match response {
                Ok(resp) => {
                    if resp
                        .status
                        .as_ref()
                        .is_some_and(|s| s.code == Code::Aborted as i32)
                    {
                        aborted = true;
                        break;
                    }
                    process_batch_write_response(&resp, &mut seen_ok, &mut seen_err);
                }
                Err(e) if e.status().map(|s| s.code) == Some(Code::Aborted) => {
                    aborted = true;
                    break;
                }
                Err(e) => return Err(e.into()),
            }
        }

        if aborted {
            if attempts >= MAX_ATTEMPTS {
                anyhow::bail!(
                    "BatchWrite failed after {} attempts due to Aborted in stream",
                    attempts
                );
            }
            sleep(Duration::from_millis(rand::random_range(10_u64..=50_u64))).await;
            continue;
        }

        break;
    }

    assert!(seen_ok, "Expected Group 1 to be processed");
    assert!(seen_err, "Expected Group 2 to be processed");

    // Verify partial commit: Group 1 exists, Group 2 does not exist
    let read_tx = db_client.single_use().build();
    let stmt = Statement::builder("SELECT ColString FROM AllTypes WHERE Id = @id_ok")
        .add_param("id_ok", &id_ok)
        .build();
    let mut result_set = read_tx.execute_query(stmt).await?;
    let row = result_set
        .next()
        .await
        .transpose()?
        .expect("Expected Group 1 committed row");
    assert_eq!(
        row.get::<String, _>("ColString"),
        "batch-write-ok",
        "Expected committed ColString value to match exactly"
    );

    Ok(())
}

fn process_batch_write_response(
    resp: &BatchWriteResponse,
    seen_ok: &mut bool,
    seen_err: &mut bool,
) {
    for &index in &resp.indexes {
        if index == 0 {
            // Group 1 (AllTypes) must succeed
            if let Some(status) = &resp.status {
                assert_eq!(
                    status.code,
                    Code::Ok as i32,
                    "Expected Group 1 (index 0) status to be OK, got: {}",
                    status.message
                );
            }
            *seen_ok = true;
        } else if index == 1 {
            // Group 2 (NonExistentTable) must fail with a non-OK code (e.g. NotFound)
            let status = resp
                .status
                .as_ref()
                .expect("Expected failure status for Group 2");
            assert_ne!(
                status.code,
                Code::Ok as i32,
                "Expected Group 2 (index 1) status to fail, got OK"
            );
            *seen_err = true;
        }
    }
}
