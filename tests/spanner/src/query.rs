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

use crate::client::{
    get_database_id, get_emulator_host, get_real_spanner_config, update_database_ddl,
};
use crate::test_proxy::{InterceptionResult, PassThroughProxy};
use futures::future::BoxFuture;
use google_cloud_spanner::client::{DatabaseClient, Spanner};
use google_cloud_spanner::model::execute_sql_request::{QueryMode, QueryOptions};
use google_cloud_spanner::model::result_set_stats::RowCount;
use google_cloud_spanner::result::Row;
use google_cloud_spanner::statement::Statement;
use google_cloud_spanner::transaction::BeginTransactionOption;
use google_cloud_spanner::value::{Kind, TypeCode};
use google_cloud_test_utils::resource_names::LowercaseAlphanumeric;
use std::sync::Arc;
use tokio::sync::Notify;
use tonic::transport::{Channel, ClientTlsConfig};
use tracing::info;

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
    let metadata = rs.metadata().expect("metadata available");
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
    let metadata_zero_rows = rs_zero_rows.metadata().expect("metadata available");
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
    let metadata_dup = rs_dup.metadata().expect("metadata available");
    assert_eq!(
        metadata_dup.column_names(),
        &["dup".to_string(), "dup".to_string()]
    );

    let val: i64 = row_dup.get("dup");
    assert_eq!(val, 1);

    Ok(())
}

pub async fn multi_use_read_only_transaction(db_client: &DatabaseClient) -> anyhow::Result<()> {
    for option in [
        BeginTransactionOption::InlineBegin,
        BeginTransactionOption::ExplicitBegin,
    ] {
        test_multi_use_read_only_transaction(db_client, option).await?;
    }
    Ok(())
}

async fn test_multi_use_read_only_transaction(
    db_client: &DatabaseClient,
    begin_transaction_option: BeginTransactionOption,
) -> anyhow::Result<()> {
    // Start a multi-use read-only transaction.
    let tx = db_client
        .read_only_transaction()
        .with_begin_transaction_option(begin_transaction_option)
        .build()
        .await?;

    if begin_transaction_option == BeginTransactionOption::ExplicitBegin {
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

pub async fn multi_use_read_only_transaction_interleaved(
    db_client: &DatabaseClient,
) -> anyhow::Result<()> {
    let tx = db_client
        .read_only_transaction()
        .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
        .build()
        .await?;

    // This test verifies that if the first query that includes the BeginTransaction
    // option does not call ResultSet#next(), then the transaction ID is still received.
    // This means that the second query on this transaction is not blocked.
    let mut rs1 = tx
        .execute_query(Statement::builder("SELECT 1 AS col_int").build())
        .await?;

    let mut rs2 = tx
        .execute_query(Statement::builder("SELECT 2 AS col_int").build())
        .await?;

    let row2 = rs2.next().await.transpose()?.expect("should yield a row");
    assert_eq!(row2.raw_values()[0].as_string(), "2");

    let row1 = rs1.next().await.transpose()?.expect("should yield a row");
    assert_eq!(row1.raw_values()[0].as_string(), "1");

    Ok(())
}

pub async fn multi_use_read_only_transaction_invalid_query_fallback(
    db_client: &DatabaseClient,
) -> anyhow::Result<()> {
    // Start a multi-use read-only transaction with implicit begin.
    let tx = db_client
        .read_only_transaction()
        .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
        .build()
        .await?;

    // Expect a read timestamp to NOT have been chosen yet.
    assert!(tx.read_timestamp().is_none());

    // Execute the first query with an invalid table name.
    // The error can be returned both directly from execute_query(...)
    // (this happens on the Emulator) or when the first row is read
    // (this happens on the real Spanner).
    let rs_result: Result<(), google_cloud_spanner::Error> = match tx
        .execute_query(Statement::builder("SELECT * FROM NonExistentTable").build())
        .await
    {
        Ok(mut rs) => match rs.next().await {
            Some(Err(e)) => Err(e),
            Some(Ok(_)) => Ok(()),
            None => Ok(()),
        },
        Err(e) => Err(e),
    };

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

fn verify_null_row(row: &Row) {
    let raw_values = row.raw_values();
    assert_eq!(raw_values.len(), 20, "Row should have exactly 20 columns");
    assert!(
        raw_values.iter().all(|v| v.kind() == Kind::Null),
        "Expected all columns to be NULL"
    );
}

fn verify_row_1(row: &Row) {
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

fn verify_row_2(row: &Row) {
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

// This test verifies that the client correctly falls back to `BeginTransaction` when the
// first statement in a transaction fails. It also shows that the statement is retried and
// could (theoretically) succeed during this retry. It achieves this by doing the following:
// 1. It uses a proxy that allows it to intercept the RPCs that are being sent to Spanner.
// 2. It creates a read-only transaction that uses inline-begin-transaction.
// 3. It executes a query that tries to read from a table that does not exist.
// 4. As the first statement in the transaction fails, the client falls back to using
//    an explicit BeginTransaction RPC.
// 5. The proxy blocks this BeginTransaction RPC, and in the meantime the test creates
//    the missing table.
// 6. The proxy unblocks the BeginTransaction RPC.
// 7. The statement is retried and succeeds. The test never sees the error.
//
// This test might seem like an extreme corner case for a read-only transaction like this.
// However, for read/write transactions, similar types of failures are more likely to occur,
// for example if a transaction tries to insert a row that violates the primary key. Another
// transaction could delete the row in the time between the first attempt failed, and the
// BeginTransaction RPC has been executed.
pub async fn inline_begin_fallback(_db_client: &DatabaseClient) -> anyhow::Result<()> {
    let destination_endpoint = match get_emulator_host() {
        Some(host) => format!("http://{}", host),
        None => "https://spanner.googleapis.com:443".to_string(),
    };

    let (project_id, instance_id) = match get_real_spanner_config() {
        Some((project, instance)) => (project, instance),
        None => ("test-project".to_string(), "test-instance".to_string()),
    };

    let latch = Arc::new(Notify::new());
    let begin_transaction_entered_latch = Arc::new(Notify::new());

    // Create a raw gRPC client that connects to the Spanner Emulator or real Spanner.
    // This will be used by the proxy server to forward requests.
    let endpoint = Channel::from_shared(destination_endpoint.clone())?;
    let endpoint = if destination_endpoint.starts_with("https") {
        endpoint.tls_config(ClientTlsConfig::new().with_enabled_roots())?
    } else {
        endpoint
    };
    let endpoint = endpoint.connect().await?;
    let latch_clone = Arc::clone(&latch);
    let begin_entered_clone = Arc::clone(&begin_transaction_entered_latch);

    // Define an interceptor that intercepts `BeginTransaction` requests,
    // notifies the test that the request has been received, and blocks
    // until the test allows it to proceed. All other requests are passed through.
    let interceptor = move |req: http::Request<tonic::body::Body>| {
        let l = latch_clone.clone();
        let be = begin_entered_clone.clone();
        Box::pin(async move {
            if req.uri().path() == "/google.spanner.v1.Spanner/BeginTransaction" {
                be.notify_one();
                l.notified().await;
            }
            InterceptionResult::Continue(req)
        }) as BoxFuture<'static, InterceptionResult>
    };

    let uri = destination_endpoint.parse::<http::Uri>()?;
    let scheme = uri
        .scheme()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("missing scheme"))?;
    let authority = uri
        .authority()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("missing authority"))?;

    let proxy = PassThroughProxy::new(endpoint, scheme, authority, interceptor);
    let proxy_server = proxy.start("127.0.0.1:0").await?;

    // We build the Spanner DatabaseClient pointing directly to our proxy address over gRPC.
    let proxy_db_client = Spanner::builder()
        .with_endpoint(proxy_server.uri())
        .build()
        .await?
        .database_client(format!(
            "projects/{}/instances/{}/databases/{}",
            project_id,
            instance_id,
            get_database_id().await
        ))
        .build()
        .await?;

    let tx = proxy_db_client
        .read_only_transaction()
        .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
        .build()
        .await?;

    let table_name = LowercaseAlphanumeric.random_string(10);
    let table_name = format!("LateLoadedTable_{}", table_name);

    // Create a task that tries to query the table before it exists.
    // This will initially fail, and the client will fall back to using
    // an explicit BeginTransaction RPC. The table will then be created
    // BEFORE the BeginTransaction RPC is executed, which will cause the
    // query to succeed when it is retried using the transaction ID that
    // was returned by BeginTransaction. This task will never see the
    // initial error, and instead it will seem like the query simply
    // succeeded.
    let query_task = tokio::spawn({
        let table_name = table_name.clone();
        async move {
            let stmt = Statement::builder(format!("SELECT * FROM {}", table_name)).build();
            let mut rs = tx.execute_query(stmt).await?;
            let _ = rs.next().await;
            Ok::<_, anyhow::Error>(tx)
        }
    });

    // Wait until the query task above has been executed and has triggered an
    // explicit BeginTransaction RPC. The BeginTransaction RPC is blocked until
    // `latch` is notified.
    begin_transaction_entered_latch.notified().await;

    // Create the table on the emulator while the BeginTransaction RPC is blocked.
    let statement = format!("CREATE TABLE {} (Id INT64) PRIMARY KEY (Id)", table_name);
    update_database_ddl(statement).await?;

    // Unblock the BeginTransaction RPC.
    latch.notify_one();

    // Wait for the query task to complete. It should succeed and never see
    // the initial error.
    let tx = query_task.await??;

    assert!(
        tx.read_timestamp().is_some(),
        "The transaction should have a read timestamp"
    );

    Ok(())
}

pub async fn query_with_options(db_client: &DatabaseClient) -> anyhow::Result<()> {
    let rot = db_client.single_use().build();

    let sql = "SELECT 1";
    let query_options = QueryOptions::default().set_optimizer_version("1");
    let stmt = Statement::builder(sql)
        .set_query_options(query_options)
        .build();

    let mut rs = rot.execute_query(stmt).await?;
    let row = rs.next().await.transpose()?.expect("should yield a row");
    let val: i64 = row.get(0);
    assert_eq!(val, 1);

    Ok(())
}

pub async fn query_plan(db_client: &DatabaseClient) -> anyhow::Result<()> {
    let rot = db_client.single_use().build();

    let sql = "SELECT 1 as num";
    let stmt = Statement::builder(sql)
        .set_query_mode(QueryMode::Plan)
        .build();

    let mut rs = rot.execute_query(stmt).await?;
    let next = rs.next().await.transpose()?;
    assert!(next.is_none());

    let metadata = rs.metadata().expect("metadata available");
    assert_eq!(metadata.column_names(), &["num".to_string()]);

    let stats = rs.stats();
    if get_emulator_host().is_some() {
        assert!(stats.is_none(), "Emulator returns no stats in PLAN mode");
    } else {
        assert!(stats.is_some(), "Real Spanner returns stats in PLAN mode");
        assert!(
            stats.unwrap().query_plan.is_some(),
            "Real Spanner returns query_plan in PLAN mode"
        );
    }

    Ok(())
}

pub async fn query_profile(db_client: &DatabaseClient) -> anyhow::Result<()> {
    if get_emulator_host().is_some() {
        info!("Skipping query_profile test on emulator");
        return Ok(());
    }
    let rot = db_client.single_use().build();

    let sql = "SELECT 1 as num";
    let stmt = Statement::builder(sql)
        .set_query_mode(QueryMode::Profile)
        .build();

    let mut rs = rot.execute_query(stmt).await?;
    let mut rows = Vec::new();
    while let Some(row) = rs.next().await {
        rows.push(row?);
    }
    assert_eq!(rows.len(), 1);

    let stats = rs.stats().expect("Expected stats in PROFILE mode");
    assert!(stats.query_plan.is_some());
    assert!(stats.query_stats.is_some());

    Ok(())
}

pub async fn dml_plan(db_client: &DatabaseClient) -> anyhow::Result<()> {
    let runner = db_client.read_write_transaction().build().await?;

    runner
        .run(async |tx| {
            let sql = "UPDATE AllTypes SET ColBool = TRUE WHERE Id = @id";
            let stmt = Statement::builder(sql)
                .set_query_mode(QueryMode::Plan)
                .build();

            let mut rs = tx.execute_query(stmt).await?;
            let next = rs.next().await.transpose()?;
            assert!(next.is_none());

            let metadata = rs.metadata().expect("metadata should be available");
            assert!(metadata.column_names().is_empty());

            // Verify undeclared parameters
            let undeclared = metadata.undeclared_parameters();
            assert_eq!(undeclared.len(), 1);
            let param_type = undeclared.get("id").expect("missing param 'id'");
            assert_eq!(param_type.code(), TypeCode::String);

            let stats = rs.stats().expect("Expected stats in PLAN mode for DML");
            assert_eq!(stats.row_count, Some(RowCount::RowCountExact(0)));

            Ok(())
        })
        .await?;

    Ok(())
}
