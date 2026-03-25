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

use google_cloud_spanner::client::{DatabaseClient, KeyRange, KeySet, Mutation, ReadRequest};
use google_cloud_spanner::key;
use google_cloud_test_utils::resource_names::LowercaseAlphanumeric;

pub async fn read_single_key(db_client: &DatabaseClient) -> anyhow::Result<()> {
    let id1 = format!("read-single-{}", LowercaseAlphanumeric.random_string(10));
    let mutation = Mutation::new_insert_or_update_builder("AllTypes")
        .set("Id")
        .to(&id1)
        .set("ColString")
        .to(&"single")
        .build();
    let write_tx = db_client.write_only_transaction().build();
    write_tx
        .write_at_least_once(vec![mutation])
        .await
        .expect("Failed to write to AllTypes");

    let read = ReadRequest::builder("AllTypes", vec!["Id", "ColString"])
        .with_keys(key![id1.clone()])
        .build();
    let mut result_set = db_client
        .single_use()
        .build()
        .execute_read(read)
        .await
        .expect("Failed to execute read single key");
    let mut rows = Vec::new();
    while let Some(row) = result_set
        .next()
        .await
        .transpose()
        .expect("Failed to get row")
    {
        rows.push(row);
    }
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String, _>("Id"), id1);
    assert_eq!(rows[0].get::<String, _>("ColString"), "single");
    Ok(())
}

pub async fn read_all_keys(db_client: &DatabaseClient) -> anyhow::Result<()> {
    // Write multiple rows
    let id1 = format!("read-all-1-{}", LowercaseAlphanumeric.random_string(10));
    let id2 = format!("read-all-2-{}", LowercaseAlphanumeric.random_string(10));
    let mutation_1 = Mutation::new_insert_or_update_builder("AllTypes")
        .set("Id")
        .to(&id1)
        .set("ColString")
        .to(&"first")
        .build();
    let mutation_2 = Mutation::new_insert_or_update_builder("AllTypes")
        .set("Id")
        .to(&id2)
        .set("ColString")
        .to(&"second")
        .build();
    let write_tx = db_client.write_only_transaction().build();
    write_tx
        .write_at_least_once(vec![mutation_1, mutation_2])
        .await
        .expect("Failed to write to AllTypes");

    let read = ReadRequest::builder("AllTypes", vec!["Id", "ColString"])
        .with_keys(KeySet::all())
        .build();
    let mut result_set = db_client
        .single_use()
        .build()
        .execute_read(read)
        .await
        .expect("Failed to execute read all keys");
    let mut rows = Vec::new();
    while let Some(row) = result_set
        .next()
        .await
        .transpose()
        .expect("Failed to get row")
    {
        let id = row.get::<String, _>("Id");
        // The table is shared across tests, so KeySet::all() may return rows
        // inserted by other concurrent tests. Filter in-memory to find the rows
        // created by this specific test.
        if id == id1 || id == id2 {
            rows.push(row);
        }
    }
    assert_eq!(rows.len(), 2);
    Ok(())
}

pub async fn read_key_range(db_client: &DatabaseClient) -> anyhow::Result<()> {
    // Write multiple rows that can be sorted lexicographically
    let prefix = format!("read-range-{}", LowercaseAlphanumeric.random_string(10));
    let id1 = format!("{}-a", prefix);
    let id2 = format!("{}-b", prefix);
    let id3 = format!("{}-c", prefix);

    let mutation_1 = Mutation::new_insert_or_update_builder("AllTypes")
        .set("Id")
        .to(&id1)
        .set("ColString")
        .to(&"first")
        .build();
    let mutation_2 = Mutation::new_insert_or_update_builder("AllTypes")
        .set("Id")
        .to(&id2)
        .set("ColString")
        .to(&"second")
        .build();
    let mutation_3 = Mutation::new_insert_or_update_builder("AllTypes")
        .set("Id")
        .to(&id3)
        .set("ColString")
        .to(&"third")
        .build();

    let write_tx = db_client.write_only_transaction().build();
    write_tx
        .write_at_least_once(vec![mutation_1, mutation_2, mutation_3])
        .await
        .expect("Failed to write to AllTypes");

    // Read range from id1 (inclusive) to id3 (exclusive) -> should return id1 and id2
    let keyset = KeySet::builder()
        .add_range(KeyRange::closed_open(key![id1.clone()], key![id3.clone()]))
        .build();

    let read = ReadRequest::builder("AllTypes", vec!["Id", "ColString"])
        .with_keys(keyset)
        .build();
    let mut result_set = db_client
        .single_use()
        .build()
        .execute_read(read)
        .await
        .expect("Failed to execute read key range");
    let mut rows = Vec::new();
    while let Some(row) = result_set
        .next()
        .await
        .transpose()
        .expect("Failed to get row")
    {
        rows.push(row);
    }
    assert_eq!(rows.len(), 2);

    let actual_ids = vec![
        rows[0].get::<String, _>("Id"),
        rows[1].get::<String, _>("Id"),
    ];
    assert_eq!(actual_ids, vec![id1, id2]);

    Ok(())
}

pub async fn read_with_limit(db_client: &DatabaseClient) -> anyhow::Result<()> {
    let prefix = format!("read-limit-{}", LowercaseAlphanumeric.random_string(10));
    let id1 = format!("{}-a", prefix);
    let id2 = format!("{}-b", prefix);
    let id3 = format!("{}-c", prefix);

    let mutation_1 = Mutation::new_insert_or_update_builder("AllTypes")
        .set("Id")
        .to(&id1)
        .set("ColString")
        .to(&"first")
        .build();
    let mutation_2 = Mutation::new_insert_or_update_builder("AllTypes")
        .set("Id")
        .to(&id2)
        .set("ColString")
        .to(&"second")
        .build();
    let mutation_3 = Mutation::new_insert_or_update_builder("AllTypes")
        .set("Id")
        .to(&id3)
        .set("ColString")
        .to(&"third")
        .build();

    let write_tx = db_client.write_only_transaction().build();
    write_tx
        .write_at_least_once(vec![mutation_1, mutation_2, mutation_3])
        .await
        .expect("Failed to write to AllTypes");

    let keyset = KeySet::builder()
        .add_range(KeyRange::closed_closed(
            key![id1.clone()],
            key![id3.clone()],
        ))
        .build();

    let read = ReadRequest::builder("AllTypes", vec!["Id", "ColString"])
        .with_keys(keyset)
        .with_limit(2) // limit to 2 rows
        .build();

    let mut result_set = db_client
        .single_use()
        .build()
        .execute_read(read)
        .await
        .expect("Failed to execute read with limit");
    let mut rows = Vec::new();
    while let Some(row) = result_set
        .next()
        .await
        .transpose()
        .expect("Failed to get row")
    {
        rows.push(row);
    }
    assert_eq!(rows.len(), 2);

    Ok(())
}

pub async fn read_with_index(db_client: &DatabaseClient) -> anyhow::Result<()> {
    let prefix = format!("read-index-{}", LowercaseAlphanumeric.random_string(10));
    let id1 = format!("{}-a", prefix);

    // We will search by ColString
    let col_string_val = format!("idx-val-{}", prefix);

    let mutation = Mutation::new_insert_or_update_builder("AllTypes")
        .set("Id")
        .to(&id1)
        .set("ColString")
        .to(&col_string_val)
        .build();

    let write_tx = db_client.write_only_transaction().build();
    write_tx
        .write_at_least_once(vec![mutation])
        .await
        .expect("Failed to write to AllTypes for index test");

    let read = ReadRequest::builder("AllTypes", vec!["Id", "ColString"])
        .with_index("Idx_AllTypes_ColString", key![col_string_val.clone()])
        .build();

    let mut result_set = db_client
        .single_use()
        .build()
        .execute_read(read)
        .await
        .expect("Failed to execute read with index");

    let mut rows = Vec::new();
    while let Some(row) = result_set
        .next()
        .await
        .transpose()
        .expect("Failed to get row")
    {
        rows.push(row);
    }
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String, _>("Id"), id1);
    assert_eq!(rows[0].get::<String, _>("ColString"), col_string_val);

    Ok(())
}
