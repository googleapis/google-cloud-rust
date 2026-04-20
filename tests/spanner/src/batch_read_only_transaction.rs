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

use crate::client::create_database_client;
use google_cloud_spanner::Partition;
use google_cloud_spanner::client::{DatabaseClient, KeySet, Mutation, ReadRequest, Statement};
use google_cloud_spanner::{PartitionExecuteOptions, PartitionOptions, key};
use google_cloud_test_utils::resource_names::LowercaseAlphanumeric;
use serde_json;

pub async fn partitioned_query(db_client: &DatabaseClient) -> anyhow::Result<()> {
    let run_id = LowercaseAlphanumeric.random_string(10);
    let id1 = format!("batch-read-1-{}", run_id);
    let id2 = format!("batch-read-2-{}", run_id);
    let id3 = format!("batch-read-3-{}", run_id);

    // 1. Insert a few test rows.
    let mutations = vec![
        Mutation::new_insert_or_update_builder("AllTypes")
            .set("Id")
            .to(&id1)
            .set("ColInt64")
            .to(&1_i64)
            .set("ColString")
            .to(&"Value 1".to_string())
            .build(),
        Mutation::new_insert_or_update_builder("AllTypes")
            .set("Id")
            .to(&id2)
            .set("ColInt64")
            .to(&2_i64)
            .set("ColString")
            .to(&"Value 2".to_string())
            .build(),
        Mutation::new_insert_or_update_builder("AllTypes")
            .set("Id")
            .to(&id3)
            .set("ColInt64")
            .to(&3_i64)
            .set("ColString")
            .to(&"Value 3".to_string())
            .build(),
    ];
    let write_tx = db_client.write_only_transaction().build();
    write_tx.write_at_least_once(mutations).await?;

    // 2. Create a BatchReadOnlyTransaction.
    let read_tx = db_client.batch_read_only_transaction().build().await?;

    // 3. Create a query that selects all the test rows and partition it.
    let hint = "@{spanner_emulator.disable_query_partitionability_check=true}";
    let sql = format!(
        "{} SELECT Id, ColInt64, ColString FROM AllTypes WHERE Id IN ('{}', '{}', '{}') ORDER BY ColInt64",
        hint, id1, id2, id3
    );
    let stmt = Statement::builder(sql).build();
    let partitions = read_tx
        .partition_query(stmt, PartitionOptions::default())
        .await?;

    // Serialize and deserialize the partitions to verify they can be passed around.
    let serialized = serde_json::to_string(&partitions)?;
    let partitions: Vec<Partition> = serde_json::from_str(&serialized)?;

    // 4. Create a new database client and execute the partitions using it.
    //    This shows that the partitions are independent from the client that
    //    created them.
    let execution_client = create_database_client()
        .await
        .expect("Failed to create executor database client");

    let mut rows_received = 0;
    for partition in partitions {
        let mut rs = partition
            .execute(&execution_client, PartitionExecuteOptions::default())
            .await?;
        while let Some(row) = rs.next().await.transpose()? {
            rows_received += 1;

            // 5. Verify that we received the rows correctly.
            let id: String = row.get("Id");
            let col_int64: i64 = row.get("ColInt64");
            let col_string: String = row.get("ColString");

            let expected_id = format!("batch-read-{}-{}", col_int64, run_id);
            let expected_str = format!("Value {}", col_int64);

            assert_eq!(id, expected_id);
            assert_eq!(col_string, expected_str);
        }
    }

    assert_eq!(rows_received, 3, "Expected to receive exactly 3 rows");

    // Clean up
    let cleanup_tx = db_client.write_only_transaction().build();
    let delete_mutations = vec![
        Mutation::delete("AllTypes", key![id1].into()),
        Mutation::delete("AllTypes", key![id2].into()),
        Mutation::delete("AllTypes", key![id3].into()),
    ];
    cleanup_tx.write_at_least_once(delete_mutations).await?;

    Ok(())
}

pub async fn partitioned_read(db_client: &DatabaseClient) -> anyhow::Result<()> {
    let run_id = LowercaseAlphanumeric.random_string(10);
    let id1 = format!("batch-read-1-{}", run_id);
    let id2 = format!("batch-read-2-{}", run_id);
    let id3 = format!("batch-read-3-{}", run_id);

    // 1. Insert a few test rows.
    let mutations = vec![
        Mutation::new_insert_or_update_builder("AllTypes")
            .set("Id")
            .to(&id1)
            .set("ColInt64")
            .to(&1_i64)
            .set("ColString")
            .to(&"Value 1".to_string())
            .build(),
        Mutation::new_insert_or_update_builder("AllTypes")
            .set("Id")
            .to(&id2)
            .set("ColInt64")
            .to(&2_i64)
            .set("ColString")
            .to(&"Value 2".to_string())
            .build(),
        Mutation::new_insert_or_update_builder("AllTypes")
            .set("Id")
            .to(&id3)
            .set("ColInt64")
            .to(&3_i64)
            .set("ColString")
            .to(&"Value 3".to_string())
            .build(),
    ];
    let write_tx = db_client.write_only_transaction().build();
    write_tx.write_at_least_once(mutations).await?;

    // 2. Create a BatchReadOnlyTransaction.
    let read_tx = db_client.batch_read_only_transaction().build().await?;

    // 3. Create a read that selects all the test rows and partition it.
    let keyset = KeySet::builder()
        .add_key(key![id1.clone()])
        .add_key(key![id2.clone()])
        .add_key(key![id3.clone()])
        .build();
    let req = ReadRequest::builder("AllTypes", vec!["Id", "ColInt64", "ColString"])
        .with_keys(keyset)
        .build();
    let partitions = read_tx
        .partition_read(req, PartitionOptions::default())
        .await?;

    // Serialize and deserialize the partitions to verify they can be passed around.
    let serialized = serde_json::to_string(&partitions)?;
    let partitions: Vec<Partition> = serde_json::from_str(&serialized)?;

    // 4. Create a new database client and execute the partitions using it.
    //    This shows that the partitions are independent from the client that
    //    created them.
    let execution_client = create_database_client()
        .await
        .expect("Failed to create executor database client");

    let mut rows_received = 0;
    for partition in partitions {
        let mut rs = partition
            .execute(&execution_client, PartitionExecuteOptions::default())
            .await?;
        while let Some(row) = rs.next().await.transpose()? {
            rows_received += 1;

            // 5. Verify that we received the rows correctly.
            let id: String = row.get("Id");
            let col_int64: i64 = row.get("ColInt64");
            let col_string: String = row.get("ColString");

            let expected_id = format!("batch-read-{}-{}", col_int64, run_id);
            let expected_str = format!("Value {}", col_int64);

            assert_eq!(id, expected_id);
            assert_eq!(col_string, expected_str);
        }
    }

    assert_eq!(rows_received, 3, "Expected to receive exactly 3 rows");

    // Clean up
    let cleanup_tx = db_client.write_only_transaction().build();
    let delete_mutations = vec![
        Mutation::delete("AllTypes", key![id1].into()),
        Mutation::delete("AllTypes", key![id2].into()),
        Mutation::delete("AllTypes", key![id3].into()),
    ];
    cleanup_tx.write_at_least_once(delete_mutations).await?;

    Ok(())
}
