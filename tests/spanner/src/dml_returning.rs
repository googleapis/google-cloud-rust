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
use google_cloud_spanner::client::{DatabaseClient, Mutation, Statement};
use google_cloud_test_utils::resource_names::LowercaseAlphanumeric;

pub async fn dml_then_return_execute_query(db_client: &DatabaseClient) -> Result<()> {
    let run_id = LowercaseAlphanumeric.random_string(10);
    let id = format!("dml-ret-q-{}", run_id);

    // 1. Insert initial test row
    let write_tx = db_client.write_only_transaction().build();
    let mutation = Mutation::new_insert_builder("AllTypes")
        .set("Id")
        .to(&id)
        .set("ColBool")
        .to(&false)
        .build();
    write_tx.write_at_least_once(vec![mutation]).await?;

    // 2. Execute DML with THEN RETURN via execute_query in a Read-Write transaction
    let runner = db_client.read_write_transaction().build().await?;
    let result = runner
        .run(async |tx| {
            let id = id.clone();
            let stmt = Statement::builder(
                "UPDATE AllTypes SET ColBool = true WHERE Id = @id THEN RETURN Id, ColBool",
            )
            .add_param("id", &id)
            .build();

            let mut result_set = tx.execute_query(stmt).await?;
            let row = result_set
                .next()
                .await
                .transpose()?
                .expect("Expected to find returned DML row");

            let returned_id: String = row.get("Id");
            let col_bool: bool = row.get("ColBool");

            assert_eq!(returned_id, id, "Row ID mismatch");
            assert!(col_bool, "ColBool should have been updated to true");

            // Verify that stats / update_count are available after fully consuming the stream
            assert!(
                result_set.next().await.is_none(),
                "Expected no additional rows"
            );
            let update_count = result_set
                .update_count()
                .expect("Expected update_count to be populated");
            assert_eq!(update_count, 1, "Expected exactly 1 row updated");

            Ok(())
        })
        .await;

    result?;
    Ok(())
}

pub async fn dml_then_return_execute_update(db_client: &DatabaseClient) -> Result<()> {
    let run_id = LowercaseAlphanumeric.random_string(10);
    let id = format!("dml-ret-u-{}", run_id);

    // 1. Insert initial test row
    let write_tx = db_client.write_only_transaction().build();
    let mutation = Mutation::new_insert_builder("AllTypes")
        .set("Id")
        .to(&id)
        .set("ColBool")
        .to(&false)
        .build();
    write_tx.write_at_least_once(vec![mutation]).await?;

    // 2. Execute DML with THEN RETURN via execute_update in a Read-Write transaction
    let runner = db_client.read_write_transaction().build().await?;
    let result = runner
        .run(async |tx| {
            let id = id.clone();
            let stmt = Statement::builder(
                "UPDATE AllTypes SET ColBool = true WHERE Id = @id THEN RETURN Id, ColBool",
            )
            .add_param("id", &id)
            .build();

            let update_count = tx.execute_update(stmt).await?;
            assert_eq!(
                update_count, 1,
                "Expected execute_update to return exactly 1 modified row count"
            );

            Ok(())
        })
        .await;

    result?;
    Ok(())
}

pub async fn dml_then_return_unconsumed_query(db_client: &DatabaseClient) -> Result<()> {
    let run_id = LowercaseAlphanumeric.random_string(10);
    let id = format!("dml-ret-uncon-{}", run_id);

    // 1. Execute DML with THEN RETURN via execute_query, but do NOT read any returned rows!
    let runner = db_client.read_write_transaction().build().await?;
    let result = runner
        .run(async |tx| {
            let id = id.clone();
            let stmt = Statement::builder(
                "INSERT INTO AllTypes (Id, ColBool) VALUES (@id, @bool) THEN RETURN Id",
            )
            .add_param("id", &id)
            .add_param("bool", &true)
            .build();

            // Execute but deliberately do not consume the rows
            let _result_set = tx.execute_query(stmt).await?;

            Ok(())
        })
        .await;

    result?;

    // 2. Issue a separate single-use read transaction to prove that Spanner successfully executed it anyway E2E
    let read_tx = db_client.single_use().build();
    let stmt = Statement::builder("SELECT Id, ColBool FROM AllTypes WHERE Id = @id")
        .add_param("id", &id)
        .build();
    let mut result_set = read_tx.execute_query(stmt).await?;
    let row = result_set
        .next()
        .await
        .transpose()?
        .expect("Expected to find row inserted by unconsumed query");

    let returned_id: String = row.get("Id");
    let col_bool: bool = row.get("ColBool");

    assert_eq!(returned_id, id, "Row ID mismatch");
    assert!(col_bool, "ColBool must be true");

    Ok(())
}
