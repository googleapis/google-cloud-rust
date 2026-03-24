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
use google_cloud_test_utils::resource_names::LowercaseAlphanumeric;

pub async fn partitioned_dml_update(db_client: &DatabaseClient) -> anyhow::Result<()> {
    let id1 = LowercaseAlphanumeric.random_string(20);
    let id2 = LowercaseAlphanumeric.random_string(20);
    let id3 = LowercaseAlphanumeric.random_string(20);

    // 1. Insert some test data
    let write_tx = db_client.write_only_transaction().build();
    let mutations = vec![
        Mutation::new_insert_or_update_builder("AllTypes")
            .set("Id")
            .to(&id1)
            .set("ColBool")
            .to(&true)
            .build(),
        Mutation::new_insert_or_update_builder("AllTypes")
            .set("Id")
            .to(&id2)
            .set("ColBool")
            .to(&false)
            .build(),
        Mutation::new_insert_or_update_builder("AllTypes")
            .set("Id")
            .to(&id3)
            .set("ColBool")
            .to(&false)
            .build(),
    ];
    write_tx.write(mutations).await?;

    // 2. Execute partitioned DML
    let pdml_tx = db_client.partitioned_dml_transaction().build().await?;
    let stmt =
        Statement::builder("UPDATE AllTypes SET ColBool = true WHERE ColBool = false").build();
    let updated_count = pdml_tx.execute_update(stmt).await?;

    // Partitioned DML returns lower bound, which should be at most 2 for the records we just inserted.
    // Spanner (and the Emulator) usually return the exact count for small numbers of rows.
    assert!(
        updated_count <= 2,
        "Expected update_count <= 2, got {}",
        updated_count
    );

    // 3. Verify
    let read_tx = db_client.single_use().build();
    let stmt = Statement::builder(
        "SELECT Id, ColBool FROM AllTypes WHERE Id IN (@id1, @id2, @id3) ORDER BY Id",
    )
    .add_param("id1", &id1)
    .add_param("id2", &id2)
    .add_param("id3", &id3)
    .build();
    let mut rs = read_tx.execute_query(stmt).await?;

    let mut rows = Vec::new();
    while let Some(row) = rs.next().await {
        rows.push(row?);
    }

    assert_eq!(rows.len(), 3, "Expected 3 rows in result set");
    for row in rows {
        let col_bool: bool = row.get("ColBool");
        let id: String = row.get("Id");
        assert!(
            col_bool,
            "All rows should have ColBool = true, but failed for Id {}",
            id
        );
    }

    Ok(())
}
