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

use google_cloud_spanner::client::{
    BeginTransactionOption, DatabaseClient, Mutation, Statement, TimestampBound,
};
use google_cloud_test_utils::resource_names::LowercaseAlphanumeric;
use google_cloud_wkt::Timestamp as WktTimestamp;
use std::time::{Duration, SystemTime};
use time::OffsetDateTime;

pub async fn read_only_bounded_staleness(db_client: &DatabaseClient) -> anyhow::Result<()> {
    // 1. Strong read-only transaction (Default / Strong)
    let tx = db_client
        .read_only_transaction()
        .with_timestamp_bound(TimestampBound::strong())
        .build()
        .await?;

    let mut rs = tx
        .execute_query(Statement::builder("SELECT 1").build())
        .await?;
    assert!(rs.next().await.transpose()?.is_some());
    assert!(
        tx.read_timestamp().is_some(),
        "Expected read_timestamp to be present for strong read-only transaction"
    );

    // 2. Exact past read_timestamp (ReadTimestamp)
    // Select the current timestamp from Spanner server to use as our exact past read timestamp.
    let mut rs = db_client
        .single_use()
        .build()
        .execute_query(Statement::builder("SELECT CURRENT_TIMESTAMP()").build())
        .await?;
    let row = rs
        .next()
        .await
        .transpose()?
        .expect("Expected current timestamp row");
    // TODO(#5684): Add FromValue/ToValue trait implementations for wkt::Timestamp to avoid conversion boilerplate.
    let spanner_now: OffsetDateTime = row.get(0);
    let spanner_now_wkt = WktTimestamp::try_from(spanner_now).expect("valid wkt timestamp");

    // Insert a new row in a read-write transaction.
    let id = format!("read-ts-{}", LowercaseAlphanumeric.random_string(10));
    let runner = db_client.read_write_transaction().build().await?;
    let commit_res = runner
        .run(async |tx| {
            let mutation = Mutation::new_insert_builder("AllTypes")
                .set("Id")
                .to(&id)
                .set("ColInt64")
                .to(&999_i64)
                .build();
            tx.buffer(vec![mutation])?;
            Ok(())
        })
        .await?;

    let commit_ts = commit_res
        .commit_response
        .commit_timestamp
        .expect("Expected commit timestamp");

    // Verify that the commit timestamp is strictly after the spanner_now timestamp.
    assert!(
        commit_ts > spanner_now_wkt,
        "Expected commit timestamp ({:?}) to be strictly after Spanner current time ({:?})",
        commit_ts,
        spanner_now_wkt
    );

    // Query the database at the exact past timestamp.
    let tx = db_client
        .read_only_transaction()
        .with_timestamp_bound(TimestampBound::read_timestamp(spanner_now))
        .build()
        .await?;

    let stmt = Statement::builder("SELECT ColInt64 FROM AllTypes WHERE Id = @id")
        .add_param("id", &id)
        .build();
    let mut rs = tx.execute_query(stmt).await?;
    let row_opt = rs.next().await.transpose()?;

    // The query must not find the row because it did not exist yet at spanner_now.
    assert!(
        row_opt.is_none(),
        "Expected row to not be found at the exact past read_timestamp"
    );

    // 3. Exact staleness (1 second)
    let tx = db_client
        .read_only_transaction()
        .with_timestamp_bound(TimestampBound::exact_staleness(Duration::from_secs(1)))
        .build()
        .await?;

    let mut rs = tx
        .execute_query(Statement::builder("SELECT 1").build())
        .await?;
    assert!(rs.next().await.transpose()?.is_some());
    let read_ts = tx
        .read_timestamp()
        .expect("Expected read_timestamp to be present for exact_staleness read-only transaction");

    // Verify that the chosen read timestamp is at least 1 second in the past compared to Spanner's current time.
    let mut rs = db_client
        .single_use()
        .build()
        .execute_query(Statement::builder("SELECT CURRENT_TIMESTAMP()").build())
        .await?;
    let row = rs
        .next()
        .await
        .transpose()?
        .expect("Expected current timestamp row");
    // TODO(#5728): Add FromValue/ToValue trait implementations for wkt::Timestamp to avoid conversion boilerplate.
    let spanner_now: OffsetDateTime = row.get(0);
    let spanner_now_wkt = WktTimestamp::try_from(spanner_now).expect("valid wkt timestamp");

    assert!(
        read_ts < spanner_now_wkt,
        "Expected read timestamp ({:?}) to be strictly in the past compared to Spanner current time ({:?})",
        read_ts,
        spanner_now_wkt
    );

    // 4. Min read timestamp (5 seconds in the past)
    // Note: min_read_timestamp can only be set for single-use read-only transactions.
    let now_minus_5 = SystemTime::now() - Duration::from_secs(5);
    let tx = db_client
        .single_use()
        .with_timestamp_bound(TimestampBound::min_read_timestamp(OffsetDateTime::from(
            now_minus_5,
        )))
        .build();

    let mut rs = tx
        .execute_query(Statement::builder("SELECT 1").build())
        .await?;
    assert!(rs.next().await.transpose()?.is_some());

    // 5. Max staleness (5 seconds)
    // Note: max_staleness can only be set for single-use read-only transactions.
    let tx = db_client
        .single_use()
        .with_timestamp_bound(TimestampBound::max_staleness(Duration::from_secs(5)))
        .build();

    let mut rs = tx
        .execute_query(Statement::builder("SELECT 1").build())
        .await?;
    assert!(rs.next().await.transpose()?.is_some());

    Ok(())
}

pub async fn read_timestamp_unavailable_before_start(
    db_client: &DatabaseClient,
) -> anyhow::Result<()> {
    let tx = db_client.read_only_transaction().build().await?;

    // Since we used InlineBegin and have not executed any query, transaction ID has not been fetched.
    assert!(
        tx.read_timestamp().is_none(),
        "Expected read_timestamp to be None before any query executes in InlineBegin transaction"
    );

    let mut rs = tx
        .execute_query(Statement::builder("SELECT 1").build())
        .await?;

    // Verify that the read timestamp is available immediately after query execution,
    // even before any rows are fetched from the stream.
    assert!(
        tx.read_timestamp().is_some(),
        "Expected read_timestamp to be populated immediately after execute_query but before fetching rows"
    );

    let row = rs.next().await.transpose()?.expect("Expected row");
    let val: i64 = row.get(0);
    assert_eq!(val, 1, "Expected query value to be 1");

    Ok(())
}

pub async fn read_timestamp_available_on_failed_first_query(
    db_client: &DatabaseClient,
) -> anyhow::Result<()> {
    let tx = db_client
        .read_only_transaction()
        .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
        .build()
        .await?;

    // First query fails because the table does not exist.
    let result = tx
        .execute_query(Statement::builder("SELECT * FROM NonExistentTable").build())
        .await;

    assert!(
        result.is_err()
            || match result {
                Ok(mut rs) => rs.next().await.transpose().is_err(),
                Err(_) => true,
            },
        "Expected first query to fail due to non-existent table"
    );

    // The read timestamp MUST be available immediately after the failed start,
    // because the client fell back to an explicit BeginTransaction RPC which successfully started the transaction.
    assert!(
        tx.read_timestamp().is_some(),
        "Expected read_timestamp to be populated after a failed query start due to explicit begin fallback"
    );

    // Verify that the transaction can still be used for subsequent valid queries.
    let mut rs2 = tx
        .execute_query(Statement::builder("SELECT 2 AS col").build())
        .await?;
    let row = rs2.next().await.transpose()?.expect("Expected row");
    let val: i64 = row.get(0);
    assert_eq!(val, 2, "Expected query value to be 2");

    Ok(())
}
