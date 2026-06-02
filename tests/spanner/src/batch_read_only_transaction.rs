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
use google_cloud_spanner::batch::Partition;
use google_cloud_spanner::client::DatabaseClient;
use google_cloud_spanner::key;
use google_cloud_spanner::model::PartitionOptions;
use google_cloud_spanner::{KeySet, Mutation, ReadRequest, Statement};
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
        "{} SELECT Id, ColInt64, ColString FROM AllTypes WHERE Id IN (@id1, @id2, @id3)",
        hint
    );
    let stmt = Statement::builder(sql)
        .add_param("id1", &id1)
        .add_param("id2", &id2)
        .add_param("id3", &id3)
        .build();
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
        let mut rs = partition.execute(&execution_client).await?;
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
        let mut rs = partition.execute(&execution_client).await?;
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

    Ok(())
}

pub async fn partition_tuning_and_data_boost(db_client: &DatabaseClient) -> anyhow::Result<()> {
    let run_id = LowercaseAlphanumeric.random_string(10);
    let ids: Vec<String> = (1..=5)
        .map(|i| format!("batch-boost-{}-{}", i, run_id))
        .collect();

    // 1. Insert 5 rows
    let mutations: Vec<Mutation> = ids
        .iter()
        .enumerate()
        .map(|(idx, id)| {
            Mutation::new_insert_or_update_builder("AllTypes")
                .set("Id")
                .to(id)
                .set("ColInt64")
                .to(&(idx as i64 + 1))
                .set("ColString")
                .to(&format!("Boost Value {}", idx + 1))
                .build()
        })
        .collect();
    db_client
        .write_only_transaction()
        .build()
        .write_at_least_once(mutations)
        .await?;

    // 2. Create BatchReadOnlyTransaction
    let read_tx = db_client.batch_read_only_transaction().build().await?;

    // 3. Partition a query with custom options
    let hint = "@{spanner_emulator.disable_query_partitionability_check=true}";
    let sql = format!(
        "{} SELECT Id, ColInt64 FROM AllTypes WHERE Id >= @start_id AND Id <= @end_id",
        hint
    );
    let stmt = Statement::builder(sql)
        .add_param("start_id", &format!("batch-boost-1-{}", run_id))
        .add_param("end_id", &format!("batch-boost-5-{}", run_id))
        .build();
    let options = PartitionOptions::default()
        .set_partition_size_bytes(512)
        .set_max_partitions(5);
    let partitions = read_tx.partition_query(stmt, options).await?;

    // 4. Execute E2E with Data Boost enabled
    let execution_client = create_database_client()
        .await
        .expect("Failed to create executor database client");
    let mut rows_received = 0;
    for partition in partitions {
        let boosted_partition = partition.set_data_boost(true);
        let mut result_set = boosted_partition.execute(&execution_client).await?;
        while let Some(row) = result_set.next().await.transpose()? {
            rows_received += 1;
            let col_int64: i64 = row.get("ColInt64");
            assert!((1..=5).contains(&col_int64));
        }
    }
    assert_eq!(
        rows_received, 5,
        "Expected to receive exactly 5 rows via boosted execution"
    );

    Ok(())
}

pub async fn parallel_partition_execution(db_client: &DatabaseClient) -> anyhow::Result<()> {
    let run_id = LowercaseAlphanumeric.random_string(10);
    let ids: Vec<String> = (1..=20)
        .map(|i| format!("batch-parallel-{:02}-{}", i, run_id))
        .collect();

    // 1. Insert 20 rows to ensure partition generation is fully testable
    let mutations: Vec<Mutation> = ids
        .iter()
        .enumerate()
        .map(|(idx, id)| {
            Mutation::new_insert_or_update_builder("AllTypes")
                .set("Id")
                .to(id)
                .set("ColInt64")
                .to(&(idx as i64 + 1))
                .build()
        })
        .collect();
    db_client
        .write_only_transaction()
        .build()
        .write_at_least_once(mutations)
        .await?;

    // 2. Create BatchReadOnlyTransaction
    let read_tx = db_client.batch_read_only_transaction().build().await?;

    // 3. Partition the query with custom sizing options
    let hint = "@{spanner_emulator.disable_query_partitionability_check=true}";
    let sql = format!(
        "{} SELECT Id, ColInt64 FROM AllTypes WHERE Id >= @start_id AND Id <= @end_id",
        hint
    );
    let stmt = Statement::builder(sql)
        .add_param("start_id", &format!("batch-parallel-01-{}", run_id))
        .add_param("end_id", &format!("batch-parallel-20-{}", run_id))
        .build();
    let options = PartitionOptions::default()
        .set_partition_size_bytes(256)
        .set_max_partitions(10);
    let partitions = read_tx.partition_query(stmt, options).await?;

    // 4. Spawn parallel tasks using tokio::task::JoinSet to run E2E executions concurrently
    let execution_client = create_database_client()
        .await
        .expect("Failed to create executor database client");
    let mut join_set = tokio::task::JoinSet::new();
    for partition in partitions {
        let client = execution_client.clone();
        join_set.spawn(async move {
            let mut result_set = partition.execute(&client).await?;
            let mut received_keys = Vec::new();
            while let Some(row) = result_set.next().await.transpose()? {
                let col_int64: i64 = row.get("ColInt64");
                received_keys.push(col_int64);
            }
            Ok::<_, anyhow::Error>(received_keys)
        });
    }

    let mut all_received_keys = Vec::new();
    while let Some(res) = join_set.join_next().await {
        let keys = res??;
        all_received_keys.extend(keys);
    }

    all_received_keys.sort();
    assert_eq!(
        all_received_keys.len(),
        20,
        "Expected exactly 20 keys back across parallel threads"
    );
    for (idx, key) in all_received_keys.into_iter().enumerate() {
        assert_eq!(key, idx as i64 + 1);
    }

    Ok(())
}
