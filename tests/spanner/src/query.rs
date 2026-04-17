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

use google_cloud_spanner::client::{DatabaseClient, Kind, QueryOptions, Statement};

pub async fn simple_query(db_client: &DatabaseClient) -> anyhow::Result<()> {
    let rot = db_client.single_use().build();

    let sql = r#"
SELECT
  1 AS col_int64,
  CAST(1.0 AS FLOAT64) AS col_float64,
  CAST(1.0 AS FLOAT32) AS col_float32,
  TRUE AS col_bool,
  'One' AS col_string,
  CAST('One' AS BYTES) AS col_bytes,
  JSON '{"value": 1}' AS col_json,
  NUMERIC '1.0' AS col_numeric,
  CAST('2026-03-09' AS DATE) AS col_date,
  CAST('2026-03-09T16:20:00Z' AS TIMESTAMP) AS col_timestamp,
  [1] AS col_array_int64,
  [CAST(1.0 AS FLOAT64)] AS col_array_float64,
  [CAST(1.0 AS FLOAT32)] AS col_array_float32,
  [TRUE] AS col_array_bool,
  ['One'] AS col_array_string,
  [CAST('One' AS BYTES)] AS col_array_bytes,
  [JSON '{"value": 1}'] AS col_array_json,
  [NUMERIC '1.0'] AS col_array_numeric,
  [CAST('2026-03-09' AS DATE)] AS col_array_date,
  [CAST('2026-03-09T16:20:00Z' AS TIMESTAMP)] AS col_array_timestamp
UNION ALL
SELECT
  2 AS col_int64,
  CAST(2.0 AS FLOAT64) AS col_float64,
  CAST(2.0 AS FLOAT32) AS col_float32,
  FALSE AS col_bool,
  'Two' AS col_string,
  CAST('Two' AS BYTES) AS col_bytes,
  JSON '{"value": 2}' AS col_json,
  NUMERIC '2.0' AS col_numeric,
  CAST('2026-03-10' AS DATE) AS col_date,
  CAST('2026-03-10T16:20:00Z' AS TIMESTAMP) AS col_timestamp,
  [2, 3] AS col_array_int64,
  [CAST(2.0 AS FLOAT64), CAST(3.0 AS FLOAT64)] AS col_array_float64,
  [CAST(2.0 AS FLOAT32), CAST(3.0 AS FLOAT32)] AS col_array_float32,
  [FALSE, TRUE] AS col_array_bool,
  ['Two', 'Three'] AS col_array_string,
  [CAST('Two' AS BYTES), CAST('Three' AS BYTES)] AS col_array_bytes,
  [JSON '{"value": 2}', JSON '{"value": 3}'] AS col_array_json,
  [NUMERIC '2.0', NUMERIC '3.0'] AS col_array_numeric,
  [CAST('2026-03-10' AS DATE), CAST('2026-03-11' AS DATE)] AS col_array_date,
  [CAST('2026-03-10T16:20:00Z' AS TIMESTAMP), CAST('2026-03-11T16:20:00Z' AS TIMESTAMP)] AS col_array_timestamp
UNION ALL
SELECT
  CAST(NULL AS INT64) AS col_int64,
  CAST(NULL AS FLOAT64) AS col_float64,
  CAST(NULL AS FLOAT32) AS col_float32,
  CAST(NULL AS BOOL) AS col_bool,
  CAST(NULL AS STRING) AS col_string,
  CAST(NULL AS BYTES) AS col_bytes,
  CAST(NULL AS JSON) AS col_json,
  CAST(NULL AS NUMERIC) AS col_numeric,
  CAST(NULL AS DATE) AS col_date,
  CAST(NULL AS TIMESTAMP) AS col_timestamp,
  CAST(NULL AS ARRAY<INT64>) AS col_array_int64,
  CAST(NULL AS ARRAY<FLOAT64>) AS col_array_float64,
  CAST(NULL AS ARRAY<FLOAT32>) AS col_array_float32,
  CAST(NULL AS ARRAY<BOOL>) AS col_array_bool,
  CAST(NULL AS ARRAY<STRING>) AS col_array_string,
  CAST(NULL AS ARRAY<BYTES>) AS col_array_bytes,
  CAST(NULL AS ARRAY<JSON>) AS col_array_json,
  CAST(NULL AS ARRAY<NUMERIC>) AS col_array_numeric,
  CAST(NULL AS ARRAY<DATE>) AS col_array_date,
  CAST(NULL AS ARRAY<TIMESTAMP>) AS col_array_timestamp
ORDER BY col_int64
"#;

    let stmt = Statement::builder(sql).build();
    let mut rs = rot
        .execute_query(stmt)
        .await
        .expect("Failed to execute query");

    let mut rows = Vec::new();
    while let Some(row) = rs.next().await {
        rows.push(row?);
    }

    let (row1, row2, row3) = match &rows[..] {
        [r1, r2, r3] => (r1, r2, r3),
        _ => panic!(
            "unexpected number of rows, got={}, want=3\n{rows:?}",
            rows.len()
        ),
    };

    // Spanner sorts NULLs first.
    verify_null_row(row1);
    verify_row_1(row2);
    verify_row_2(row3);

    Ok(())
}

pub async fn query_with_parameters(db_client: &DatabaseClient) -> anyhow::Result<()> {
    let rot = db_client.single_use().build();

    let sql = r#"
    WITH Data AS (
        SELECT 1 as id, 'Alice' as name 
        UNION ALL 
        SELECT 2 as id, 'Bob' as name
    ) 
    SELECT name FROM Data WHERE id = @id
    "#;

    let stmt = Statement::builder(sql).add_param("id", &2).build();
    let mut rs = rot
        .execute_query(stmt)
        .await
        .expect("Failed to execute query");

    let mut rows = Vec::new();
    while let Some(row) = rs.next().await {
        rows.push(row?);
    }

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].raw_values()[0].as_string(), "Bob");

    Ok(())
}

pub async fn result_set_metadata(db_client: &DatabaseClient) -> anyhow::Result<()> {
    let rot = db_client.single_use().build();

    // 1. Simple normal query
    let sql = "SELECT 1 as num, 'Alice' as name";
    let mut rs = rot.execute_query(Statement::builder(sql).build()).await?;

    assert!(rs.next().await.transpose()?.is_some());
    let metadata = rs.metadata()?;
    assert_eq!(
        metadata.column_names(),
        &["num".to_string(), "name".to_string()]
    );

    // 2. Query that returns zero rows
    let sql_zero_rows = r#"
    WITH Data AS (
        SELECT 1 as num, 'Alice' as name
    )
    SELECT num, name FROM Data WHERE 1=0
    "#;
    let mut rs_zero_rows = rot
        .execute_query(Statement::builder(sql_zero_rows).build())
        .await?;

    assert!(rs_zero_rows.next().await.transpose()?.is_none());
    let metadata_zero_rows = rs_zero_rows.metadata()?;
    assert_eq!(
        metadata_zero_rows.column_names(),
        &["num".to_string(), "name".to_string()]
    );

    // 3. Query with duplicate aliases
    let sql_dup = "SELECT 1 as dup, 2 as dup";
    let mut rs_dup = rot
        .execute_query(Statement::builder(sql_dup).build())
        .await?;

    let row_dup = rs_dup.next().await.transpose()?.unwrap();
    let metadata_dup = rs_dup.metadata()?;
    assert_eq!(
        metadata_dup.column_names(),
        &["dup".to_string(), "dup".to_string()]
    );

    let val: i64 = row_dup.get("dup");
    assert_eq!(val, 1);

    Ok(())
}

pub async fn multi_use_read_only_transaction(db_client: &DatabaseClient) -> anyhow::Result<()> {
    for explicit_begin in [false, true] {
        test_multi_use_read_only_transaction(db_client, explicit_begin).await?;
    }
    Ok(())
}

async fn test_multi_use_read_only_transaction(
    db_client: &DatabaseClient,
    explicit_begin: bool,
) -> anyhow::Result<()> {
    // Start a multi-use read-only transaction.
    let tx = db_client
        .read_only_transaction()
        .with_explicit_begin_transaction(explicit_begin)
        .build()
        .await?;

    if explicit_begin {
        // Expect a read timestamp to have been chosen immediately.
        assert!(tx.read_timestamp().is_some());
    } else {
        // Expect a read timestamp to NOT have been chosen yet.
        assert!(tx.read_timestamp().is_none());
    }

    // Execute the first query.
    let mut rs1 = tx
        .execute_query(Statement::builder("SELECT 1 AS col_int").build())
        .await?;
    let row1 = rs1.next().await.transpose()?.expect("should yield a row");

    // The read timestamp is now always available.
    assert!(tx.read_timestamp().is_some());

    let val1 = row1.raw_values()[0].as_string();
    assert_eq!(val1, "1");
    let next1 = rs1.next().await.transpose()?;
    assert!(next1.is_none(), "{next1:?}");

    // Execute the second query reusing the same transaction.
    let mut rs2 = tx
        .execute_query(Statement::builder("SELECT 2 AS col_int").build())
        .await?;
    let row2 = rs2.next().await.transpose()?.expect("should yield a row");
    let val2 = row2.raw_values()[0].as_string();
    assert_eq!(val2, "2");
    let next2 = rs2.next().await.transpose()?;
    assert!(next2.is_none(), "{next2:?}");

    Ok(())
}

pub async fn multi_use_read_only_transaction_invalid_query_fallback(
    db_client: &DatabaseClient,
) -> anyhow::Result<()> {
    // Start a multi-use read-only transaction with implicit begin.
    let tx = db_client
        .read_only_transaction()
        .with_explicit_begin_transaction(false)
        .build()
        .await?;

    // Expect a read timestamp to NOT have been chosen yet.
    assert!(tx.read_timestamp().is_none());

    // Execute the first query with invalid syntax.
    let rs_result = tx
        .execute_query(Statement::builder("SELECT * FROM NonExistentTable").build())
        .await;

    assert!(
        rs_result.is_err(),
        "Expected an error from an invalid query"
    );

    // The read timestamp should now be available because the transaction
    // fell back to an explicit BeginTransaction.
    assert!(tx.read_timestamp().is_some());

    // It should be possible to use the transaction.
    let mut rs2 = tx
        .execute_query(Statement::builder("SELECT 2 AS col_int").build())
        .await?;

    let row2 = rs2.next().await.transpose()?.expect("should yield a row");
    let val2 = row2.raw_values()[0].as_string();
    assert_eq!(val2, "2");

    Ok(())
}

fn verify_null_row(row: &google_cloud_spanner::client::Row) {
    let raw_values = row.raw_values();
    assert_eq!(raw_values.len(), 20, "Row should have exactly 20 columns");
    assert!(
        raw_values.iter().all(|v| v.kind() == Kind::Null),
        "Expected all columns to be NULL"
    );
}

fn verify_row_1(row: &google_cloud_spanner::client::Row) {
    let raw_values = row.raw_values();
    assert_eq!(raw_values.len(), 20, "Row should have exactly 20 columns");
    assert_eq!(raw_values[0].as_string(), "1"); // INT64 is encoded as string
    assert_eq!(raw_values[1].as_f64(), 1.0);
    assert_eq!(raw_values[2].as_f64(), 1.0); // FLOAT32 is encoded as f64
    assert!(raw_values[3].as_bool());
    assert_eq!(raw_values[4].as_string(), "One");
    assert_eq!(raw_values[5].as_string(), "T25l"); // Base64 'One'
    assert_eq!(raw_values[6].as_string(), "{\"value\":1}"); // JSON
    assert_eq!(raw_values[7].as_string(), "1"); // NUMERIC is encoded as string
    assert_eq!(raw_values[8].as_string(), "2026-03-09");
    assert_eq!(raw_values[9].as_string(), "2026-03-09T16:20:00Z");

    assert_eq!(raw_values[10].as_list().len(), 1);
    assert_eq!(raw_values[10].as_list().get(0).unwrap().as_string(), "1");
    assert_eq!(raw_values[11].as_list().len(), 1);
    assert_eq!(raw_values[11].as_list().get(0).unwrap().as_f64(), 1.0);
    assert_eq!(raw_values[12].as_list().len(), 1);
    assert_eq!(raw_values[12].as_list().get(0).unwrap().as_f64(), 1.0);
    assert_eq!(raw_values[13].as_list().len(), 1);
    assert!(raw_values[13].as_list().get(0).unwrap().as_bool());
    assert_eq!(raw_values[14].as_list().len(), 1);
    assert_eq!(raw_values[14].as_list().get(0).unwrap().as_string(), "One");
    assert_eq!(raw_values[15].as_list().len(), 1);
    assert_eq!(raw_values[15].as_list().get(0).unwrap().as_string(), "T25l");
    assert_eq!(raw_values[16].as_list().len(), 1);
    assert_eq!(
        raw_values[16].as_list().get(0).unwrap().as_string(),
        "{\"value\":1}"
    );
    assert_eq!(raw_values[17].as_list().len(), 1);
    assert_eq!(raw_values[17].as_list().get(0).unwrap().as_string(), "1");
    assert_eq!(raw_values[18].as_list().len(), 1);
    assert_eq!(
        raw_values[18].as_list().get(0).unwrap().as_string(),
        "2026-03-09"
    );
    assert_eq!(raw_values[19].as_list().len(), 1);
    assert_eq!(
        raw_values[19].as_list().get(0).unwrap().as_string(),
        "2026-03-09T16:20:00Z"
    );
}

fn verify_row_2(row: &google_cloud_spanner::client::Row) {
    let raw_values = row.raw_values();
    assert_eq!(raw_values.len(), 20, "Row should have exactly 20 columns");
    assert_eq!(raw_values[0].as_string(), "2");
    assert_eq!(raw_values[1].as_f64(), 2.0);
    assert_eq!(raw_values[2].as_f64(), 2.0);
    assert!(!raw_values[3].as_bool());
    assert_eq!(raw_values[4].as_string(), "Two");
    assert_eq!(raw_values[5].as_string(), "VHdv"); // Base64 'Two'
    assert_eq!(raw_values[6].as_string(), "{\"value\":2}");
    assert_eq!(raw_values[7].as_string(), "2");
    assert_eq!(raw_values[8].as_string(), "2026-03-10");
    assert_eq!(raw_values[9].as_string(), "2026-03-10T16:20:00Z");

    assert_eq!(raw_values[10].as_list().len(), 2);
    assert_eq!(raw_values[10].as_list().get(0).unwrap().as_string(), "2");
    assert_eq!(raw_values[10].as_list().get(1).unwrap().as_string(), "3");
    assert_eq!(raw_values[11].as_list().len(), 2);
    assert_eq!(raw_values[11].as_list().get(0).unwrap().as_f64(), 2.0);
    assert_eq!(raw_values[11].as_list().get(1).unwrap().as_f64(), 3.0);
    assert_eq!(raw_values[12].as_list().len(), 2);
    assert_eq!(raw_values[12].as_list().get(0).unwrap().as_f64(), 2.0);
    assert_eq!(raw_values[12].as_list().get(1).unwrap().as_f64(), 3.0);
    assert_eq!(raw_values[13].as_list().len(), 2);
    assert!(!raw_values[13].as_list().get(0).unwrap().as_bool());
    assert!(raw_values[13].as_list().get(1).unwrap().as_bool());
    assert_eq!(raw_values[14].as_list().len(), 2);
    assert_eq!(raw_values[14].as_list().get(0).unwrap().as_string(), "Two");
    assert_eq!(
        raw_values[14].as_list().get(1).unwrap().as_string(),
        "Three"
    );
    assert_eq!(raw_values[15].as_list().len(), 2);
    assert_eq!(raw_values[15].as_list().get(0).unwrap().as_string(), "VHdv");
    assert_eq!(
        raw_values[15].as_list().get(1).unwrap().as_string(),
        "VGhyZWU="
    );
    assert_eq!(raw_values[16].as_list().len(), 2);
    assert_eq!(
        raw_values[16].as_list().get(0).unwrap().as_string(),
        "{\"value\":2}"
    );
    assert_eq!(
        raw_values[16].as_list().get(1).unwrap().as_string(),
        "{\"value\":3}"
    );
    assert_eq!(raw_values[17].as_list().len(), 2);
    assert_eq!(raw_values[17].as_list().get(0).unwrap().as_string(), "2");
    assert_eq!(raw_values[17].as_list().get(1).unwrap().as_string(), "3");
    assert_eq!(raw_values[18].as_list().len(), 2);
    assert_eq!(
        raw_values[18].as_list().get(0).unwrap().as_string(),
        "2026-03-10"
    );
    assert_eq!(
        raw_values[18].as_list().get(1).unwrap().as_string(),
        "2026-03-11"
    );
    assert_eq!(raw_values[19].as_list().len(), 2);
    assert_eq!(
        raw_values[19].as_list().get(0).unwrap().as_string(),
        "2026-03-10T16:20:00Z"
    );
    assert_eq!(
        raw_values[19].as_list().get(1).unwrap().as_string(),
        "2026-03-11T16:20:00Z"
    );
}

pub async fn query_with_options(db_client: &DatabaseClient) -> anyhow::Result<()> {
    let rot = db_client.single_use().build();

    let sql = "SELECT 1";
    let query_options = QueryOptions::default().set_optimizer_version("1");
    let stmt = Statement::builder(sql)
        .with_query_options(query_options)
        .build();

    let mut rs = rot.execute_query(stmt).await?;
    let row = rs.next().await.transpose()?.expect("should yield a row");
    let val: i64 = row.get(0);
    assert_eq!(val, 1);

    Ok(())
}
