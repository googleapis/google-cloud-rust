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

use google_cloud_spanner::client::DatabaseClient;
use google_cloud_spanner::key;
use google_cloud_spanner::{Mutation, ReadRequest, Statement};
use google_cloud_test_utils::resource_names::LowercaseAlphanumeric;

pub async fn successful_read_write_transaction(db_client: &DatabaseClient) -> anyhow::Result<()> {
    let id = format!("rw-success-{}", LowercaseAlphanumeric.random_string(10));

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
    let id = format!("rw-rollback-{}", LowercaseAlphanumeric.random_string(10));

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

pub async fn read_write_transaction_with_mutations(
    db_client: &DatabaseClient,
) -> anyhow::Result<()> {
    let id1 = format!("rw-mut-1-{}", LowercaseAlphanumeric.random_string(10));
    let id2 = format!("rw-mut-2-{}", LowercaseAlphanumeric.random_string(10));

    // Insert initial row for id1
    let mutation = Mutation::new_insert_builder("AllTypes")
        .set("Id")
        .to(&id1)
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
        .with_transaction_tag("mutations-tag")
        .build()
        .await?;
    runner
        .run(async |transaction| {
            // Execute select query to start transaction
            let statement = Statement::builder("SELECT ColInt64 FROM AllTypes WHERE Id = @id")
                .add_param("id", &id1)
                .build();
            let mut result_set = transaction.execute_query(statement).await?;
            let row = result_set
                .next()
                .await
                .transpose()?
                .expect("Row exists for success transaction test");
            let current_val: i64 = row.get("ColInt64");

            // Buffer a mutation for id2
            let buffer_mut = Mutation::new_insert_builder("AllTypes")
                .set("Id")
                .to(&id2)
                .set("ColInt64")
                .to(&(current_val + 100))
                .build();
            transaction.buffer([buffer_mut])?;

            Ok(())
        })
        .await?;

    // Verify buffered mutation committed successfully
    let statement = Statement::builder("SELECT ColInt64 FROM AllTypes WHERE Id = @id")
        .add_param("id", &id2)
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
    assert_eq!(
        final_val, 200,
        "Buffered mutation should have been committed"
    );

    Ok(())
}

pub async fn read_write_transaction_mutation_only(
    db_client: &DatabaseClient,
) -> anyhow::Result<()> {
    let id = format!("rw-only-mut-{}", LowercaseAlphanumeric.random_string(10));

    let mutation = Mutation::new_insert_builder("AllTypes")
        .set("Id")
        .to(&id)
        .set("ColInt64")
        .to(&555_i64)
        .build();

    let runner = db_client
        .read_write_transaction()
        .with_transaction_tag("only-mutations-tag")
        .build()
        .await?;
    runner
        .run(async |transaction| {
            transaction.buffer([mutation.clone()])?;
            Ok(())
        })
        .await?;

    // Verify committed successfully
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
    assert_eq!(
        final_val, 555,
        "Mutation-only read/write transaction should have been committed"
    );

    Ok(())
}

pub async fn read_write_transaction_multiple_queries_and_dml(
    db_client: &DatabaseClient,
) -> anyhow::Result<()> {
    let first_id = format!("rw-multi-1-{}", LowercaseAlphanumeric.random_string(10));
    let second_id = format!("rw-multi-2-{}", LowercaseAlphanumeric.random_string(10));

    // Insert two rows
    let first_mutation = Mutation::new_insert_builder("AllTypes")
        .set("Id")
        .to(&first_id)
        .set("ColInt64")
        .to(&100_i64)
        .build();
    let second_mutation = Mutation::new_insert_builder("AllTypes")
        .set("Id")
        .to(&second_id)
        .set("ColInt64")
        .to(&200_i64)
        .build();
    db_client
        .write_only_transaction()
        .build()
        .write(vec![first_mutation, second_mutation])
        .await?;

    let runner = db_client
        .read_write_transaction()
        .with_transaction_tag("multi-query-tag")
        .build()
        .await?;
    runner
        .run(async |transaction| {
            // First query
            let first_statement =
                Statement::builder("SELECT ColInt64 FROM AllTypes WHERE Id = @id")
                    .add_param("id", &first_id)
                    .build();
            let mut first_result_set = transaction.execute_query(first_statement).await?;
            let first_row = first_result_set
                .next()
                .await
                .transpose()?
                .expect("First row exists for multiple queries transaction test");
            let first_value: i64 = first_row.get("ColInt64");

            // Second query
            let second_statement =
                Statement::builder("SELECT ColInt64 FROM AllTypes WHERE Id = @id")
                    .add_param("id", &second_id)
                    .build();
            let mut second_result_set = transaction.execute_query(second_statement).await?;
            let second_row = second_result_set
                .next()
                .await
                .transpose()?
                .expect("Second row exists for multiple queries transaction test");
            let second_value: i64 = second_row.get("ColInt64");

            // DML statement
            let update_statement =
                Statement::builder("UPDATE AllTypes SET ColInt64 = @new_val WHERE Id = @id")
                    .add_param("new_val", &(first_value + second_value))
                    .add_param("id", &first_id)
                    .build();
            transaction.execute_update(update_statement).await?;

            Ok(())
        })
        .await?;

    // Verify update
    let statement = Statement::builder("SELECT ColInt64 FROM AllTypes WHERE Id = @id")
        .add_param("id", &first_id)
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
    let final_value: i64 = row.get("ColInt64");
    assert_eq!(
        final_value, 300,
        "Update from multiple queries and DML transaction should have been committed"
    );

    Ok(())
}

pub async fn consecutive_reads(db_client: &DatabaseClient) -> anyhow::Result<()> {
    let id1 = format!("rw-cr-1-{}", LowercaseAlphanumeric.random_string(10));
    let id2 = format!("rw-cr-2-{}", LowercaseAlphanumeric.random_string(10));

    // Insert initial rows
    let mutation1 = Mutation::new_insert_builder("AllTypes")
        .set("Id")
        .to(&id1)
        .set("ColInt64")
        .to(&10_i64)
        .build();
    let mutation2 = Mutation::new_insert_builder("AllTypes")
        .set("Id")
        .to(&id2)
        .set("ColInt64")
        .to(&20_i64)
        .build();
    db_client
        .write_only_transaction()
        .build()
        .write(vec![mutation1, mutation2])
        .await?;

    let runner = db_client
        .read_write_transaction()
        .with_transaction_tag("consecutive-reads-tag")
        .build()
        .await?;

    runner
        .run(async |transaction| {
            let read1 = ReadRequest::builder("AllTypes", vec!["ColInt64"])
                .with_keys(key![id1.clone()])
                .build();
            let mut result_set1 = transaction.execute_read(read1).await?;
            let row1 = result_set1
                .next()
                .await
                .transpose()?
                .expect("Row 1 exists for consecutive reads test");
            let val1: i64 = row1.get("ColInt64");
            assert_eq!(val1, 10, "Row 1 should have value 10");

            let read2 = ReadRequest::builder("AllTypes", vec!["ColInt64"])
                .with_keys(key![id2.clone()])
                .build();
            let mut result_set2 = transaction.execute_read(read2).await?;
            let row2 = result_set2
                .next()
                .await
                .transpose()?
                .expect("Row 2 exists for consecutive reads test");
            let val2: i64 = row2.get("ColInt64");
            assert_eq!(val2, 20, "Row 2 should have value 20");

            let update_statement =
                Statement::builder("UPDATE AllTypes SET ColInt64 = @new_val WHERE Id = @id")
                    .add_param("new_val", &(val1 + val2))
                    .add_param("id", &id1)
                    .build();
            transaction.execute_update(update_statement).await?;

            Ok(())
        })
        .await?;

    // Verify update
    let statement = Statement::builder("SELECT ColInt64 FROM AllTypes WHERE Id = @id")
        .add_param("id", &id1)
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
    assert_eq!(
        final_val, 30,
        "Update should have been committed after consecutive reads"
    );

    Ok(())
}

pub async fn mixed_reads_and_queries(db_client: &DatabaseClient) -> anyhow::Result<()> {
    let id1 = format!("rw-mx-1-{}", LowercaseAlphanumeric.random_string(10));
    let id2 = format!("rw-mx-2-{}", LowercaseAlphanumeric.random_string(10));

    let mutation1 = Mutation::new_insert_builder("AllTypes")
        .set("Id")
        .to(&id1)
        .set("ColInt64")
        .to(&100_i64)
        .build();
    let mutation2 = Mutation::new_insert_builder("AllTypes")
        .set("Id")
        .to(&id2)
        .set("ColInt64")
        .to(&200_i64)
        .build();
    db_client
        .write_only_transaction()
        .build()
        .write(vec![mutation1, mutation2])
        .await?;

    let runner = db_client
        .read_write_transaction()
        .with_transaction_tag("mixed-operations-tag")
        .build()
        .await?;

    runner
        .run(async |transaction| {
            let read_request = ReadRequest::builder("AllTypes", vec!["ColInt64"])
                .with_keys(key![id1.clone()])
                .build();
            let mut result_set1 = transaction.execute_read(read_request).await?;
            let row1 = result_set1
                .next()
                .await
                .transpose()?
                .expect("Row 1 exists for mixed read test");
            let val1: i64 = row1.get("ColInt64");

            let query_statement =
                Statement::builder("SELECT ColInt64 FROM AllTypes WHERE Id = @id")
                    .add_param("id", &id2)
                    .build();
            let mut result_set2 = transaction.execute_query(query_statement).await?;
            let row2 = result_set2
                .next()
                .await
                .transpose()?
                .expect("Row 2 exists for mixed query test");
            let val2: i64 = row2.get("ColInt64");

            let update_statement =
                Statement::builder("UPDATE AllTypes SET ColInt64 = @new_val WHERE Id = @id")
                    .add_param("new_val", &(val1 + val2 + 50))
                    .add_param("id", &id1)
                    .build();
            transaction.execute_update(update_statement).await?;

            Ok(())
        })
        .await?;

    // Verify update
    let statement = Statement::builder("SELECT ColInt64 FROM AllTypes WHERE Id = @id")
        .add_param("id", &id1)
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
    assert_eq!(
        final_val, 350,
        "Update should have been committed after mixed reads and queries"
    );

    Ok(())
}

pub async fn multiple_execute_updates(db_client: &DatabaseClient) -> anyhow::Result<()> {
    let id1 = format!("rw-meu-1-{}", LowercaseAlphanumeric.random_string(10));
    let id2 = format!("rw-meu-2-{}", LowercaseAlphanumeric.random_string(10));

    let mutation1 = Mutation::new_insert_builder("AllTypes")
        .set("Id")
        .to(&id1)
        .set("ColInt64")
        .to(&5_i64)
        .build();
    let mutation2 = Mutation::new_insert_builder("AllTypes")
        .set("Id")
        .to(&id2)
        .set("ColInt64")
        .to(&10_i64)
        .build();
    db_client
        .write_only_transaction()
        .build()
        .write(vec![mutation1, mutation2])
        .await?;

    let runner = db_client
        .read_write_transaction()
        .with_transaction_tag("multiple-updates-tag")
        .build()
        .await?;

    runner
        .run(async |transaction| {
            let statement1 =
                Statement::builder("UPDATE AllTypes SET ColInt64 = ColInt64 + 10 WHERE Id = @id")
                    .add_param("id", &id1)
                    .build();
            let count1 = transaction.execute_update(statement1).await?;
            assert_eq!(count1, 1, "Expected 1 row updated for statement 1");

            let statement2 =
                Statement::builder("UPDATE AllTypes SET ColInt64 = ColInt64 + 20 WHERE Id = @id")
                    .add_param("id", &id2)
                    .build();
            let count2 = transaction.execute_update(statement2).await?;
            assert_eq!(count2, 1, "Expected 1 row updated for statement 2");

            Ok(())
        })
        .await?;

    // Verify updates
    let statement =
        Statement::builder("SELECT ColInt64 FROM AllTypes WHERE Id IN (@id1, @id2) ORDER BY Id")
            .add_param("id1", &id1)
            .add_param("id2", &id2)
            .build();
    let mut result_set = db_client
        .single_use()
        .build()
        .execute_query(statement)
        .await?;

    let row1 = result_set
        .next()
        .await
        .transpose()?
        .expect("Row 1 exists for verification");
    let val1: i64 = row1.get("ColInt64");
    assert_eq!(val1, 15, "Expected updated value for id1");

    let row2 = result_set
        .next()
        .await
        .transpose()?
        .expect("Row 2 exists for verification");
    let val2: i64 = row2.get("ColInt64");
    assert_eq!(val2, 30, "Expected updated value for id2");

    Ok(())
}

pub async fn read_your_writes_consistency(db_client: &DatabaseClient) -> anyhow::Result<()> {
    let id = format!("rw-ryw-{}", LowercaseAlphanumeric.random_string(10));

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
        .with_transaction_tag("read-your-writes-tag")
        .build()
        .await?;

    runner
        .run(async |transaction| {
            let statement1 = Statement::builder("SELECT ColInt64 FROM AllTypes WHERE Id = @id")
                .add_param("id", &id)
                .build();
            let mut result_set1 = transaction.execute_query(statement1).await?;
            let row1 = result_set1
                .next()
                .await
                .transpose()?
                .expect("Initial row exists");
            let initial_val: i64 = row1.get("ColInt64");
            assert_eq!(initial_val, 100);

            let update_statement =
                Statement::builder("UPDATE AllTypes SET ColInt64 = 250 WHERE Id = @id")
                    .add_param("id", &id)
                    .build();
            let updated_count = transaction.execute_update(update_statement).await?;
            assert_eq!(updated_count, 1);

            let statement2 = Statement::builder("SELECT ColInt64 FROM AllTypes WHERE Id = @id")
                .add_param("id", &id)
                .build();
            let mut result_set2 = transaction.execute_query(statement2).await?;
            let row2 = result_set2
                .next()
                .await
                .transpose()?
                .expect("Subsequent row exists");
            let mid_transaction_val: i64 = row2.get("ColInt64");
            assert_eq!(
                mid_transaction_val, 250,
                "Transaction should reflect uncommitted DML update"
            );

            Ok(())
        })
        .await?;

    Ok(())
}

pub async fn buffered_mutation_interleaving(db_client: &DatabaseClient) -> anyhow::Result<()> {
    let id1 = format!("rw-bi-1-{}", LowercaseAlphanumeric.random_string(10));
    let id2 = format!("rw-bi-2-{}", LowercaseAlphanumeric.random_string(10));

    let mutation = Mutation::new_insert_builder("AllTypes")
        .set("Id")
        .to(&id1)
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
        .with_transaction_tag("buffered-interleaving-tag")
        .build()
        .await?;

    runner
        .run(async |transaction| {
            let buffer_mutation = Mutation::new_insert_builder("AllTypes")
                .set("Id")
                .to(&id2)
                .set("ColInt64")
                .to(&999_i64)
                .build();
            transaction.buffer([buffer_mutation])?;

            let statement = Statement::builder("UPDATE AllTypes SET ColInt64 = 200 WHERE Id = @id")
                .add_param("id", &id1)
                .build();
            transaction.execute_update(statement).await?;

            Ok(())
        })
        .await?;

    let statement =
        Statement::builder("SELECT ColInt64 FROM AllTypes WHERE Id IN (@id1, @id2) ORDER BY Id")
            .add_param("id1", &id1)
            .add_param("id2", &id2)
            .build();
    let mut result_set = db_client
        .single_use()
        .build()
        .execute_query(statement)
        .await?;

    let row1 = result_set.next().await.transpose()?.expect("Row 1 exists");
    assert_eq!(row1.get::<i64, _>("ColInt64"), 200);

    let row2 = result_set.next().await.transpose()?.expect("Row 2 exists");
    assert_eq!(row2.get::<i64, _>("ColInt64"), 999);

    Ok(())
}

pub async fn initial_statement_failure_handling(db_client: &DatabaseClient) -> anyhow::Result<()> {
    let runner = db_client
        .read_write_transaction()
        .with_transaction_tag("initial-fail-tag")
        .build()
        .await?;

    let result: google_cloud_spanner::Result<()> = runner
        .run(async |transaction| {
            let statement =
                Statement::builder("SELECT * FROM NonExistentTableForErrorHandling").build();
            let query_result = match transaction.execute_query(statement).await {
                Ok(mut result_set) => result_set.next().await.map(|res| res.map(|_| ())),
                Err(e) => Some(Err(e)),
            };
            assert!(
                query_result.as_ref().is_some_and(|r| r.is_err()),
                "Query on non-existent table should fail"
            );
            Err(query_result.unwrap().unwrap_err())
        })
        .await
        .map(|result| result.result);

    assert!(result.is_err(), "Transaction runner should propagate error");
    Ok(())
}

pub async fn intermediate_statement_constraint_error(
    db_client: &DatabaseClient,
) -> anyhow::Result<()> {
    let id1 = format!("rw-isc-1-{}", LowercaseAlphanumeric.random_string(10));
    let id2 = format!("rw-isc-2-{}", LowercaseAlphanumeric.random_string(10));

    let mutation1 = Mutation::new_insert_builder("AllTypes")
        .set("Id")
        .to(&id1)
        .set("ColInt64")
        .to(&10_i64)
        .build();
    let mutation2 = Mutation::new_insert_builder("AllTypes")
        .set("Id")
        .to(&id2)
        .set("ColInt64")
        .to(&20_i64)
        .build();
    db_client
        .write_only_transaction()
        .build()
        .write(vec![mutation1, mutation2])
        .await?;

    let runner = db_client
        .read_write_transaction()
        .with_transaction_tag("constraint-error-tag")
        .build()
        .await?;

    let result: google_cloud_spanner::Result<()> = runner
        .run(async |transaction| {
            let statement = Statement::builder("SELECT ColInt64 FROM AllTypes WHERE Id = @id")
                .add_param("id", &id1)
                .build();
            let mut result_set = transaction.execute_query(statement).await?;
            let _row = result_set.next().await.transpose()?.expect("Row exists");

            // Attempt to insert a row with an ID that already exists to cause a constraint violation (primary key duplicate)
            let invalid_insert =
                Statement::builder("INSERT INTO AllTypes (Id, ColInt64) VALUES (@id, 50)")
                    .add_param("id", &id2)
                    .build();
            let execute_result = transaction.execute_update(invalid_insert).await;
            assert!(
                execute_result.is_err(),
                "Duplicate primary key insert should fail"
            );

            Err(execute_result.unwrap_err())
        })
        .await
        .map(|res| res.result);

    assert!(
        result.is_err(),
        "Transaction should return an error due to primary key violation"
    );
    Ok(())
}

pub async fn buffered_mutation_commit_rejection(db_client: &DatabaseClient) -> anyhow::Result<()> {
    let id = format!("rw-bmc-{}", LowercaseAlphanumeric.random_string(10));

    let initial_mutation = Mutation::new_insert_builder("AllTypes")
        .set("Id")
        .to(&id)
        .set("ColInt64")
        .to(&100_i64)
        .build();
    db_client
        .write_only_transaction()
        .build()
        .write(vec![initial_mutation])
        .await?;

    let runner = db_client
        .read_write_transaction()
        .with_transaction_tag("commit-rejection-tag")
        .build()
        .await?;

    let result: google_cloud_spanner::Result<()> = runner
        .run(async |transaction| {
            let statement = Statement::builder("SELECT ColInt64 FROM AllTypes WHERE Id = @id")
                .add_param("id", &id)
                .build();
            let mut result_set = transaction.execute_query(statement).await?;
            let _row = result_set.next().await.transpose()?.expect("Row exists");

            // Buffer an insert with the exact same primary key to trigger error upon commit
            let duplicate_mutation = Mutation::new_insert_builder("AllTypes")
                .set("Id")
                .to(&id)
                .set("ColInt64")
                .to(&500_i64)
                .build();
            transaction.buffer([duplicate_mutation])?;

            Ok(())
        })
        .await
        .map(|res| res.result);

    assert!(
        result.is_err(),
        "Commit should fail when applying buffered mutation with duplicate key"
    );
    Ok(())
}

pub async fn application_error_explicit_rollback(db_client: &DatabaseClient) -> anyhow::Result<()> {
    let id1 = format!("rw-aeer-1-{}", LowercaseAlphanumeric.random_string(10));
    let id2 = format!("rw-aeer-2-{}", LowercaseAlphanumeric.random_string(10));

    let initial_mutation = Mutation::new_insert_builder("AllTypes")
        .set("Id")
        .to(&id1)
        .set("ColInt64")
        .to(&100_i64)
        .build();
    db_client
        .write_only_transaction()
        .build()
        .write(vec![initial_mutation])
        .await?;

    let runner = db_client
        .read_write_transaction()
        .with_transaction_tag("explicit-rollback-tag")
        .build()
        .await?;

    let result: google_cloud_spanner::Result<()> = runner
        .run(async |transaction| {
            let update_statement =
                Statement::builder("UPDATE AllTypes SET ColInt64 = 200 WHERE Id = @id")
                    .add_param("id", &id1)
                    .build();
            transaction.execute_update(update_statement).await?;

            let buffered_mutation = Mutation::new_insert_builder("AllTypes")
                .set("Id")
                .to(&id2)
                .set("ColInt64")
                .to(&300_i64)
                .build();
            transaction.buffer([buffered_mutation])?;

            Err(google_cloud_spanner::Error::io(std::io::Error::other(
                "Application determined rollback",
            )))
        })
        .await
        .map(|res| res.result);

    assert!(result.is_err(), "Expected application error");

    // Verify update was rolled back and buffered mutation was discarded
    let statement1 = Statement::builder("SELECT ColInt64 FROM AllTypes WHERE Id = @id")
        .add_param("id", &id1)
        .build();
    let mut result_set1 = db_client
        .single_use()
        .build()
        .execute_query(statement1)
        .await?;
    let row1 = result_set1
        .next()
        .await
        .transpose()?
        .expect("Row 1 exists for verification");
    assert_eq!(
        row1.get::<i64, _>("ColInt64"),
        100,
        "Update statement should be rolled back"
    );

    let statement2 = Statement::builder("SELECT ColInt64 FROM AllTypes WHERE Id = @id")
        .add_param("id", &id2)
        .build();
    let mut result_set2 = db_client
        .single_use()
        .build()
        .execute_query(statement2)
        .await?;
    assert!(
        result_set2.next().await.transpose()?.is_none(),
        "Buffered mutation should have been discarded"
    );

    Ok(())
}

pub async fn continue_after_initial_query_error(db_client: &DatabaseClient) -> anyhow::Result<()> {
    let id = format!("rw-con-err-{}", LowercaseAlphanumeric.random_string(10));

    let runner = db_client
        .read_write_transaction()
        .with_transaction_tag("continue-after-err-tag")
        .build()
        .await?;

    runner
        .run(async |transaction| {
            // 1. Query from a table that does not exist. Catch the error and proceed with the transaction.
            let invalid_statement =
                Statement::builder("SELECT * FROM NonExistentTableToTestContinuation").build();
            let query_result = match transaction.execute_query(invalid_statement).await {
                Ok(mut result_set) => result_set.next().await.map(|res| res.map(|_| ())),
                Err(e) => Some(Err(e)),
            };
            assert!(
                query_result.as_ref().is_some_and(|r| r.is_err()),
                "Query on non-existent table should fail"
            );

            // 2. Insert a row into a table that does exist.
            let valid_insert =
                Statement::builder("INSERT INTO AllTypes (Id, ColInt64) VALUES (@id, 777)")
                    .add_param("id", &id)
                    .build();
            let insert_result = transaction.execute_update(valid_insert).await?;
            assert_eq!(insert_result, 1, "Expected 1 row inserted");

            Ok(())
        })
        .await?;

    // Verify that the insert actually worked
    let verify_statement = Statement::builder("SELECT ColInt64 FROM AllTypes WHERE Id = @id")
        .add_param("id", &id)
        .build();
    let mut result_set = db_client
        .single_use()
        .build()
        .execute_query(verify_statement)
        .await?;
    let row = result_set
        .next()
        .await
        .transpose()?
        .expect("Row exists for verification");
    assert_eq!(
        row.get::<i64, _>("ColInt64"),
        777,
        "Insert should have succeeded despite earlier query error"
    );

    Ok(())
}
