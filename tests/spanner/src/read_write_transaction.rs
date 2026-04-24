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

use google_cloud_spanner::client::{DatabaseClient, Mutation, Statement};

pub async fn successful_read_write_transaction(db_client: &DatabaseClient) -> anyhow::Result<()> {
    let id = format!(
        "rw-success-{}",
        google_cloud_test_utils::resource_names::LowercaseAlphanumeric.random_string(10)
    );

    // Insert a row
    let mutation = Mutation::new_insert_builder("AllTypes")
        .set("Id")
        .to(&id)
        .set("ColInt64")
        .to(&100_i64)
        .build();
    db_client
        .write_only_transaction()
        .build()
        .write(vec![mutation])
        .await?;

    let runner = db_client
        .read_write_transaction()
        .with_transaction_tag("success-tag")
        .build()
        .await?;
    runner
        .run(async |transaction| {
            let statement = Statement::builder("SELECT ColInt64 FROM AllTypes WHERE Id = @id")
                .add_param("id", &id)
                .with_request_tag("select-tag")
                .build();
            let mut result_set = transaction.execute_query(statement).await?;
            let row = result_set
                .next()
                .await
                .transpose()?
                .expect("Row exists for success transaction test");
            let current_val: i64 = row.get("ColInt64");

            let update_statement =
                Statement::builder("UPDATE AllTypes SET ColInt64 = @new_val WHERE Id = @id")
                    .add_param("new_val", &(current_val + 50))
                    .add_param("id", &id)
                    .with_request_tag("update-tag")
                    .build();
            transaction.execute_update(update_statement).await?;

            Ok(())
        })
        .await?;

    // Verify
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
        .expect("Row exists for verification");
    let final_val: i64 = row.get("ColInt64");
    assert_eq!(final_val, 150, "Update should have been committed");

    Ok(())
}

pub async fn rolled_back_read_write_transaction(db_client: &DatabaseClient) -> anyhow::Result<()> {
    let id = format!(
        "rw-rollback-{}",
        google_cloud_test_utils::resource_names::LowercaseAlphanumeric.random_string(10)
    );

    // Insert a row
    let mutation = Mutation::new_insert_builder("AllTypes")
        .set("Id")
        .to(&id)
        .set("ColInt64")
        .to(&100_i64)
        .build();
    db_client
        .write_only_transaction()
        .build()
        .write(vec![mutation])
        .await?;

    let runner = db_client
        .read_write_transaction()
        .with_transaction_tag("rollback-tag")
        .build()
        .await?;
    let res: google_cloud_spanner::Result<()> = runner
        .run(async |transaction| {
            let statement = Statement::builder("SELECT ColInt64 FROM AllTypes WHERE Id = @id")
                .add_param("id", &id)
                .with_request_tag("select-tag")
                .build();
            let mut result_set = transaction.execute_query(statement).await?;
            let row = result_set
                .next()
                .await
                .transpose()?
                .expect("Row exists for rollback transaction test");
            let current_val: i64 = row.get("ColInt64");

            let update_statement =
                Statement::builder("UPDATE AllTypes SET ColInt64 = @new_val WHERE Id = @id")
                    .add_param("new_val", &(current_val + 50))
                    .add_param("id", &id)
                    .with_request_tag("update-tag")
                    .build();
            transaction.execute_update(update_statement).await?;

            Err(google_cloud_spanner::Error::io(std::io::Error::other(
                "Simulated error to trigger rollback",
            )))
        })
        .await
        .map(|res| res.result);

    assert!(res.is_err(), "Transaction should return an error");

    // Verify rollback
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
        .expect("Row exists for verification");
    let final_val: i64 = row.get("ColInt64");
    assert_eq!(final_val, 100, "Update should have been rolled back");

    Ok(())
}

pub async fn concurrent_read_write_transaction_retries(
    db_client: &DatabaseClient,
) -> anyhow::Result<()> {
    use futures::future::join_all;
    use google_cloud_test_utils::resource_names::LowercaseAlphanumeric;
    use std::sync::Arc;
    use tokio::sync::Barrier;

    const NUM_ROWS: usize = 10;

    let id_prefix = format!("rw-retry-{}", LowercaseAlphanumeric.random_string(10));

    // 1. Insert rows
    let mut mutations = Vec::new();
    for i in 0..NUM_ROWS {
        let id = format!("{}-{}", id_prefix, i);
        let mutation = Mutation::new_insert_builder("AllTypes")
            .set("Id")
            .to(&id)
            .set("ColInt64")
            .to(&100_i64)
            .build();
        mutations.push(mutation);
    }
    db_client
        .write_only_transaction()
        .build()
        .write(mutations)
        .await
        .expect("Failed to insert initial rows");

    // 2. Spawn tasks
    let mut handles = Vec::new();
    let barrier = Arc::new(Barrier::new(NUM_ROWS));

    for i in 0..NUM_ROWS {
        let client = db_client.clone();
        let id_prefix = id_prefix.clone();
        let target_idx = i;
        let b = barrier.clone();

        let handle = tokio::spawn(async move {
            b.wait().await; // Wait for all tasks to be ready

            let runner = client
                .read_write_transaction()
                .with_transaction_tag("concurrent-tag")
                .build()
                .await
                .expect("Failed to build transaction runner");

            let res: Result<(), google_cloud_spanner::Error> = runner
                .run(async move |transaction| {
                    // Read all 10 rows to take locks.
                    // NOTE: This is intentionally reading more rows than needed,
                    // in order to force transactions to conflict with each other.
                    let statement = {
                        let start_id = format!("{}-0", id_prefix);
                        let end_id = format!("{}-{}", id_prefix, NUM_ROWS - 1);
                        Statement::builder(
                            "SELECT Id FROM AllTypes WHERE Id >= @start AND Id <= @end",
                        )
                        .add_param("start", &start_id)
                        .add_param("end", &end_id)
                        .with_request_tag("concurrent-select")
                        .build()
                    };
                    let mut result_set = transaction.execute_query(statement).await?;
                    while let Some(row) = result_set.next().await.transpose()? {
                        let _: String = row.get("Id"); // Consume row
                    }

                    // Update one row
                    let update_statement = {
                        let update_id = format!("{}-{}", id_prefix, target_idx);
                        Statement::builder(
                            "UPDATE AllTypes SET ColInt64 = ColInt64 + @inc WHERE Id = @id",
                        )
                        .add_param("inc", &50_i64)
                        .add_param("id", &update_id)
                        .with_request_tag("concurrent-update")
                        .build()
                    };
                    transaction.execute_update(update_statement).await?;

                    Ok(())
                })
                .await
                .map(|res| res.result);
            res.expect("Transaction failed");
        });
        handles.push(handle);
    }

    // 3. Wait for all tasks to complete
    join_all(handles).await;

    // 4. Verify all updates
    for i in 0..NUM_ROWS {
        let id = format!("{}-{}", id_prefix, i);
        let statement = Statement::builder("SELECT ColInt64 FROM AllTypes WHERE Id = @id")
            .add_param("id", &id)
            .build();
        let mut result_set = db_client
            .single_use()
            .build()
            .execute_query(statement)
            .await
            .expect("Failed to execute verification query");
        let row = result_set
            .next()
            .await
            .transpose()?
            .unwrap_or_else(|| panic!("Row for {} does not exist", id));
        let val: i64 = row.get("ColInt64");
        assert_eq!(val, 150, "Update on {} was not applied", id);
    }

    Ok(())
}
