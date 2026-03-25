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

    let runner = db_client.read_write_transaction().build().await?;
    let _ = runner
        .run(async |transaction| {
            let statement = Statement::builder("SELECT ColInt64 FROM AllTypes WHERE Id = @id")
                .add_param("id", &id)
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

    let runner = db_client.read_write_transaction().build().await?;
    let res: google_cloud_spanner::Result<()> = runner
        .run(async |transaction| {
            let statement = Statement::builder("SELECT ColInt64 FROM AllTypes WHERE Id = @id")
                .add_param("id", &id)
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
                    .build();
            transaction.execute_update(update_statement).await?;

            Err(google_cloud_spanner::Error::io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Simulated error to trigger rollback",
            )))
        })
        .await;

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
