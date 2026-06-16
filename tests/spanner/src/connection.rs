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

use anyhow::{Context, Result};
use google_cloud_spanner::connection::{Connection, Dialect, ExecutionResult};
use google_cloud_test_utils::resource_names::LowercaseAlphanumeric;

pub async fn run_connection_tests(dsn: &str) -> Result<()> {
    let mut conn = Connection::connect(dsn)
        .await
        .context("Failed to connect via Connection API")?;

    // 1. Verify show dialect works
    let dialect = conn.state().dialect();

    // Generate unique ID for testing inserts
    let run_id = LowercaseAlphanumeric.random_string(8);
    let id_1 = format!("conn-1-{}", run_id);
    let id_2 = format!("conn-2-{}", run_id);

    // 2. Test auto-commit DML insert and DQL SELECT query
    let insert_sql = match dialect {
        Dialect::GoogleSql => format!(
            "INSERT INTO AllTypes (Id, ColString) VALUES ('{}', 'Auto-commit insert')",
            id_1
        ),
        Dialect::PostgreSql => format!(
            "INSERT INTO AllTypes (Id, ColString) VALUES ('{}', 'Auto-commit insert')",
            id_1
        ),
        _ => panic!("Unsupported dialect"),
    };

    let res = conn
        .execute(insert_sql)
        .await
        .context("Insert statement failed")?;
    if let ExecutionResult::UpdateResult(count) = res {
        assert_eq!(count, 1, "Expected exactly 1 row inserted");
    } else {
        panic!("Expected UpdateResult from INSERT");
    }

    // Verify it is inserted using SELECT
    let select_sql = format!("SELECT ColString FROM AllTypes WHERE Id = '{}'", id_1);
    let res = conn
        .execute(select_sql)
        .await
        .context("Select statement failed")?;
    if let ExecutionResult::QueryResult(mut rs) = res {
        let row = rs.next().await.transpose()?.context("Row not found")?;
        let col_val: String = row.get(0);
        assert_eq!(col_val, "Auto-commit insert");
    } else {
        panic!("Expected QueryResult from SELECT");
    }

    // 3. Test explicit transaction: BEGIN -> insert -> COMMIT
    let begin_res = conn.execute("BEGIN").await.context("BEGIN failed")?;
    assert!(matches!(begin_res, ExecutionResult::Success));

    let insert_sql_2 = format!(
        "INSERT INTO AllTypes (Id, ColString) VALUES ('{}', 'Transaction insert')",
        id_2
    );
    let res = conn
        .execute(insert_sql_2)
        .await
        .context("Insert inside transaction failed")?;
    if let ExecutionResult::UpdateResult(count) = res {
        assert_eq!(count, 1);
    } else {
        panic!("Expected UpdateResult");
    }

    let commit_res = conn.execute("COMMIT").await.context("COMMIT failed")?;
    assert!(matches!(commit_res, ExecutionResult::Success));

    // Verify the second insert is committed and exists
    let select_sql_2 = format!("SELECT ColString FROM AllTypes WHERE Id = '{}'", id_2);
    let res = conn
        .execute(select_sql_2.as_str())
        .await
        .context("Select statement 2 failed")?;
    if let ExecutionResult::QueryResult(mut rs) = res {
        let row = rs.next().await.transpose()?.context("Row 2 not found")?;
        let col_val: String = row.get(0);
        assert_eq!(col_val, "Transaction insert");
    } else {
        panic!("Expected QueryResult");
    }

    // 4. Test explicit transaction: BEGIN -> update -> ROLLBACK
    let begin_res = conn.execute("BEGIN").await.context("BEGIN 2 failed")?;
    assert!(matches!(begin_res, ExecutionResult::Success));

    let update_sql = format!(
        "UPDATE AllTypes SET ColString = 'Updated description' WHERE Id = '{}'",
        id_2
    );
    let res = conn
        .execute(update_sql)
        .await
        .context("Update inside transaction failed")?;
    if let ExecutionResult::UpdateResult(count) = res {
        assert_eq!(count, 1);
    } else {
        panic!("Expected UpdateResult");
    }

    let rollback_res = conn.execute("ROLLBACK").await.context("ROLLBACK failed")?;
    assert!(matches!(rollback_res, ExecutionResult::Success));

    // Verify update was rolled back (value is still 'Transaction insert')
    let res = conn
        .execute(select_sql_2.as_str())
        .await
        .context("Select statement after rollback failed")?;
    if let ExecutionResult::QueryResult(mut rs) = res {
        let row = rs
            .next()
            .await
            .transpose()?
            .context("Row not found after rollback")?;
        let col_val: String = row.get(0);
        assert_eq!(col_val, "Transaction insert");
    } else {
        panic!("Expected QueryResult");
    }

    // 5. Test DML returning clause / THEN RETURN
    let returning_sql = match dialect {
        Dialect::GoogleSql => format!(
            "UPDATE AllTypes SET ColString = 'Updated with returning' WHERE Id = '{}' THEN RETURN ColString",
            id_2
        ),
        Dialect::PostgreSql => format!(
            "UPDATE AllTypes SET ColString = 'Updated with returning' WHERE Id = '{}' RETURNING ColString",
            id_2
        ),
        _ => panic!("Unsupported dialect"),
    };

    let res = conn
        .execute(returning_sql)
        .await
        .context("DML Returning failed")?;
    if let ExecutionResult::QueryResult(mut rs) = res {
        let row = rs
            .next()
            .await
            .transpose()?
            .context("Returning row not found")?;
        let col_val: String = row.get(0);
        assert_eq!(col_val, "Updated with returning");
    } else {
        panic!("Expected QueryResult for DML with returning/THEN RETURN clause");
    }

    // 6. Test Batch DML execution
    let start_dml_batch = conn
        .execute("START BATCH DML")
        .await
        .context("START BATCH DML failed")?;
    assert!(matches!(start_dml_batch, ExecutionResult::Success));

    let batch_dml_1 = format!("UPDATE AllTypes SET ColInt64 = 100 WHERE Id = '{}'", id_1);
    let batch_dml_2 = format!("UPDATE AllTypes SET ColInt64 = 200 WHERE Id = '{}'", id_2);

    let res1 = conn
        .execute(batch_dml_1)
        .await
        .context("Batch DML 1 failed")?;
    assert!(matches!(res1, ExecutionResult::Success));
    let res2 = conn
        .execute(batch_dml_2)
        .await
        .context("Batch DML 2 failed")?;
    assert!(matches!(res2, ExecutionResult::Success));

    let run_dml_batch = conn
        .execute("RUN BATCH")
        .await
        .context("RUN BATCH failed")?;
    if let ExecutionResult::UpdateResult(total_updated) = run_dml_batch {
        assert_eq!(
            total_updated, 2,
            "Expected exactly 2 updates from the DML batch"
        );
    } else {
        panic!("Expected UpdateResult from RUN BATCH");
    }

    // Verify values updated by batch DML
    let select_col_int64 = format!(
        "SELECT ColInt64 FROM AllTypes WHERE Id IN ('{}', '{}') ORDER BY Id ASC",
        id_1, id_2
    );
    let res = conn
        .execute(select_col_int64)
        .await
        .context("Select batch results failed")?;
    if let ExecutionResult::QueryResult(mut rs) = res {
        let row1 = rs
            .next()
            .await
            .transpose()?
            .context("Batch result row 1 not found")?;
        assert_eq!(row1.get::<i64, usize>(0), 100);
        let row2 = rs
            .next()
            .await
            .transpose()?
            .context("Batch result row 2 not found")?;
        assert_eq!(row2.get::<i64, usize>(0), 200);
    } else {
        panic!("Expected QueryResult for batch DML verification");
    }

    Ok(())
}
