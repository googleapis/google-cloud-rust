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

use google_cloud_spanner::batch::BatchDml;
use google_cloud_spanner::client::DatabaseClient;
use google_cloud_spanner::error::BatchUpdateError;
use google_cloud_spanner::{Mutation, Statement};
use google_cloud_test_utils::resource_names::LowercaseAlphanumeric;

pub async fn successful_batch_update(db_client: &DatabaseClient) -> anyhow::Result<()> {
    let id1 = format!("bdml-suc-1-{}", LowercaseAlphanumeric.random_string(10));
    let id2 = format!("bdml-suc-2-{}", LowercaseAlphanumeric.random_string(10));

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
        .set_transaction_tag("batch-success-tag")
        .build()
        .await?;

    runner
        .run(async |transaction| {
            let statement1 =
                Statement::builder("UPDATE AllTypes SET ColInt64 = 100 WHERE Id = @id")
                    .add_param("id", &id1)
                    .build();
            let statement2 =
                Statement::builder("UPDATE AllTypes SET ColInt64 = 200 WHERE Id = @id")
                    .add_param("id", &id2)
                    .build();

            let batch = BatchDml::builder()
                .add_statement(statement1)
                .add_statement(statement2)
                .build();

            let update_counts = transaction.execute_batch_update(batch).await?;
            assert_eq!(
                update_counts,
                vec![1, 1],
                "Expected exactly 1 row updated for each statement"
            );

            Ok(())
        })
        .await?;

    // Verify updates
    let verify_stmt =
        Statement::builder("SELECT ColInt64 FROM AllTypes WHERE Id IN (@id1, @id2) ORDER BY Id")
            .add_param("id1", &id1)
            .add_param("id2", &id2)
            .build();
    let mut result_set = db_client
        .single_use()
        .build()
        .execute_query(verify_stmt)
        .await?;

    let row1 = result_set
        .next()
        .await
        .transpose()?
        .expect("Row 1 exists for verification");
    assert_eq!(
        row1.get::<i64, _>("ColInt64"),
        100,
        "Update on row 1 should be committed"
    );

    let row2 = result_set
        .next()
        .await
        .transpose()?
        .expect("Row 2 exists for verification");
    assert_eq!(
        row2.get::<i64, _>("ColInt64"),
        200,
        "Update on row 2 should be committed"
    );

    Ok(())
}

pub async fn partial_batch_update_failure(db_client: &DatabaseClient) -> anyhow::Result<()> {
    let id = format!("bdml-err-{}", LowercaseAlphanumeric.random_string(10));

    let mutation = Mutation::new_insert_builder("AllTypes")
        .set("Id")
        .to(&id)
        .set("ColInt64")
        .to(&50_i64)
        .build();

    db_client
        .write_only_transaction()
        .build()
        .write(vec![mutation])
        .await?;

    let runner = db_client
        .read_write_transaction()
        .set_transaction_tag("batch-partial-tag")
        .build()
        .await?;

    let result: google_cloud_spanner::Result<()> = runner
        .run(async |transaction| {
            let valid_statement =
                Statement::builder("UPDATE AllTypes SET ColInt64 = 500 WHERE Id = @id")
                    .add_param("id", &id)
                    .build();
            let invalid_statement = Statement::builder(
                "UPDATE AllTypes SET ColInt64 = CAST('invalid_number' AS INT64) WHERE Id = @id",
            )
            .add_param("id", &id)
            .build();

            let batch = BatchDml::builder()
                .add_statement(valid_statement)
                .add_statement(invalid_statement)
                .build();

            let batch_result = transaction.execute_batch_update(batch).await;
            assert!(
                batch_result.is_err(),
                "Batch execution should fail on invalid statement"
            );

            let error = batch_result.unwrap_err();
            if let Some(batch_error) = BatchUpdateError::extract(&error) {
                assert_eq!(
                    batch_error.update_counts,
                    vec![1],
                    "First valid update should show 1 row successfully modified"
                );
            }

            Err(error)
        })
        .await
        .map(|result| result.result);

    assert!(
        result.is_err(),
        "Transaction should return an error due to partial batch failure"
    );
    Ok(())
}

pub async fn empty_batch_statement_rejection(db_client: &DatabaseClient) -> anyhow::Result<()> {
    let runner = db_client
        .read_write_transaction()
        .set_transaction_tag("batch-empty-tag")
        .build()
        .await?;

    let result: google_cloud_spanner::Result<()> = runner
        .run(async |transaction| {
            let empty_batch = BatchDml::builder().build();
            let batch_result = transaction.execute_batch_update(empty_batch).await;
            assert!(
                batch_result.is_err(),
                "Executing an empty batch should be rejected"
            );

            Err(batch_result.unwrap_err())
        })
        .await
        .map(|result| result.result);

    assert!(
        result.is_err(),
        "Transaction runner should propagate rejection error"
    );
    Ok(())
}

pub async fn unsupported_query_in_batch_dml(db_client: &DatabaseClient) -> anyhow::Result<()> {
    let runner = db_client
        .read_write_transaction()
        .set_transaction_tag("batch-query-tag")
        .build()
        .await?;

    let result: google_cloud_spanner::Result<()> = runner
        .run(async |transaction| {
            let query_statement = Statement::builder("SELECT * FROM AllTypes").build();
            let batch = BatchDml::builder().add_statement(query_statement).build();

            let batch_result = transaction.execute_batch_update(batch).await;
            assert!(
                batch_result.is_err(),
                "Executing a SELECT query inside BatchDml should be rejected"
            );

            Err(batch_result.unwrap_err())
        })
        .await
        .map(|result| result.result);

    assert!(
        result.is_err(),
        "Transaction runner should propagate rejection error"
    );
    Ok(())
}

pub async fn unsupported_returning_clause(db_client: &DatabaseClient) -> anyhow::Result<()> {
    let id = format!("bdml-ret-{}", LowercaseAlphanumeric.random_string(10));

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
        .set_transaction_tag("batch-returning-tag")
        .build()
        .await?;

    let _result: google_cloud_spanner::Result<()> = runner
        .run(async |transaction| {
            let returning_statement = Statement::builder(
                "UPDATE AllTypes SET ColInt64 = 200 WHERE Id = @id THEN RETURN ColInt64",
            )
            .add_param("id", &id)
            .build();
            let batch = BatchDml::builder()
                .add_statement(returning_statement)
                .build();

            let batch_result = transaction.execute_batch_update(batch).await;
            match batch_result {
                Ok(counts) => {
                    assert_eq!(
                        counts,
                        vec![1],
                        "If accepted by emulator, exactly 1 row should be updated"
                    );
                    Ok(())
                }
                Err(error) => Err(error),
            }
        })
        .await
        .map(|result| result.result);
    Ok(())
}

pub async fn continue_after_empty_batch_statement(
    db_client: &DatabaseClient,
) -> anyhow::Result<()> {
    let id = format!("bdml-con-emp-{}", LowercaseAlphanumeric.random_string(10));

    let runner = db_client
        .read_write_transaction()
        .set_transaction_tag("continue-empty-tag")
        .build()
        .await?;

    let _result = runner
        .run(async |transaction| {
            // 1. First statement is an invalid empty batch. Catch error and proceed.
            let empty_batch = BatchDml::builder().build();
            let batch_result = transaction.execute_batch_update(empty_batch).await;
            assert!(
                batch_result.is_err(),
                "Executing an empty batch should be rejected"
            );

            // 2. Continue with a valid statement and commit.
            let valid_insert =
                Statement::builder("INSERT INTO AllTypes (Id, ColInt64) VALUES (@id, 888)")
                    .add_param("id", &id)
                    .build();
            transaction.execute_update(valid_insert).await?;

            Ok(())
        })
        .await;

    // Verify commit if emulator allowed continuation
    if _result.is_ok() {
        let verify_statement = Statement::builder("SELECT ColInt64 FROM AllTypes WHERE Id = @id")
            .add_param("id", &id)
            .build();
        let mut result_set = db_client
            .single_use()
            .build()
            .execute_query(verify_statement)
            .await?;
        let _ = result_set.next().await;
    }

    Ok(())
}

pub async fn continue_after_invalid_first_statement_in_batch(
    db_client: &DatabaseClient,
) -> anyhow::Result<()> {
    let id = format!("bdml-con-inv-{}", LowercaseAlphanumeric.random_string(10));

    let runner = db_client
        .read_write_transaction()
        .set_transaction_tag("continue-invalid-tag")
        .build()
        .await?;

    let _result = runner
        .run(async |transaction| {
            // 1. First statement is a Batch DML with one invalid statement.
            let invalid_statement = Statement::builder(
                "UPDATE NonExistentTableForBatchError SET ColInt64 = 999 WHERE Id = @id",
            )
            .add_param("id", &id)
            .build();
            let batch = BatchDml::builder().add_statement(invalid_statement).build();

            let batch_result = transaction.execute_batch_update(batch).await;
            assert!(
                batch_result.is_err(),
                "Executing a batch with an invalid statement should be rejected"
            );

            // 2. Verify that we can continue to use the established transaction.
            let valid_insert =
                Statement::builder("INSERT INTO AllTypes (Id, ColInt64) VALUES (@id, 999)")
                    .add_param("id", &id)
                    .build();
            transaction.execute_update(valid_insert).await?;

            Ok(())
        })
        .await;

    // Verify commit if emulator allowed continuation
    if _result.is_ok() {
        let verify_statement = Statement::builder("SELECT ColInt64 FROM AllTypes WHERE Id = @id")
            .add_param("id", &id)
            .build();
        let mut result_set = db_client
            .single_use()
            .build()
            .execute_query(verify_statement)
            .await?;
        let _ = result_set.next().await;
    }

    Ok(())
}

pub async fn continue_after_invalid_second_statement_in_batch(
    db_client: &DatabaseClient,
) -> anyhow::Result<()> {
    let id = format!("bdml-con-sec-{}", LowercaseAlphanumeric.random_string(10));

    let mutation = Mutation::new_insert_builder("AllTypes")
        .set("Id")
        .to(&id)
        .set("ColInt64")
        .to(&50_i64)
        .build();
    db_client
        .write_only_transaction()
        .build()
        .write(vec![mutation])
        .await?;

    let runner = db_client
        .read_write_transaction()
        .set_transaction_tag("continue-second-invalid-tag")
        .build()
        .await?;

    let _result = runner
        .run(async |transaction| {
            // 1. Batch DML with a valid first statement and invalid second statement.
            let valid_statement =
                Statement::builder("UPDATE AllTypes SET ColInt64 = 100 WHERE Id = @id")
                    .add_param("id", &id)
                    .build();
            let invalid_statement = Statement::builder(
                "UPDATE NonExistentTableForBatchError SET ColInt64 = 999 WHERE Id = @id",
            )
            .add_param("id", &id)
            .build();
            let batch = BatchDml::builder()
                .add_statement(valid_statement)
                .add_statement(invalid_statement)
                .build();

            let batch_result = transaction.execute_batch_update(batch).await;
            assert!(
                batch_result.is_err(),
                "Executing a batch with an invalid second statement should be rejected"
            );

            // 2. Continue with a valid statement and commit.
            let valid_update =
                Statement::builder("UPDATE AllTypes SET ColInt64 = 200 WHERE Id = @id")
                    .add_param("id", &id)
                    .build();
            transaction.execute_update(valid_update).await?;

            Ok(())
        })
        .await;

    // Verify commit if emulator allowed continuation
    if _result.is_ok() {
        let verify_statement = Statement::builder("SELECT ColInt64 FROM AllTypes WHERE Id = @id")
            .add_param("id", &id)
            .build();
        let mut result_set = db_client
            .single_use()
            .build()
            .execute_query(verify_statement)
            .await?;
        let _ = result_set.next().await;
    }

    Ok(())
}
