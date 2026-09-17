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

use crate::writes::arrow::ArrowSerializer;
use crate::writes::count_writes_table;
use anyhow::Result;
use google_cloud_bigquery::client::Write;
use google_cloud_bigquery_v2::client::TableService;
use google_cloud_bigquery_v2::model::{Table, TableReference, TableSchema};
use std::sync::Arc;

pub const FLAKY_REGION: &str = "us-east7";
const ITERATIONS: usize = 50;
const ROWS_PER_BATCH: usize = 10;
const TOTAL_ROWS: usize = ITERATIONS * ROWS_PER_BATCH; // 500

/// Tests sequential appends to a table with the `_reconnect_on_close` suffix in `us-east7`.
///
/// BigQuery drops the connection every 10 requests on this table to exercise
/// reconnect and retry logic during sequential writes.
pub async fn reconnect_on_close_sequential(
    client: &Write,
    table_service: &TableService,
    project_id: &str,
    dataset_id: &str,
    schema: TableSchema,
) -> Result<()> {
    let table_id = format!(
        "{}_reconnect_on_close",
        bigquery_samples::random_id_suffix()
    );
    create_flaky_table(table_service, project_id, dataset_id, &table_id, schema).await?;

    let table = format!("projects/{project_id}/datasets/{dataset_id}/tables/{table_id}");
    let mut serializer = ArrowSerializer::new("reconnect_seq")?;

    let writer = client.arrow(serializer.schema()).pending(table).await?;

    for i in 0..ITERATIONS {
        let offset = (i * ROWS_PER_BATCH) as i64;
        let batch = serializer.generate_batch(ROWS_PER_BATCH, i * ROWS_PER_BATCH)?;
        let resp = writer.append(batch).set_offset(offset).send().await?;
        assert_eq!(resp.offset, Some(offset));
    }

    let finalize_resp = writer.finalize().await?;
    assert_eq!(finalize_resp.row_count, TOTAL_ROWS as i64);

    let commit_resp = writer.commit().await?;
    assert!(
        commit_resp.stream_errors.is_empty(),
        "unexpected stream errors on commit: {:?}",
        commit_resp.stream_errors
    );

    let count = count_writes_table(
        project_id,
        dataset_id,
        &table_id,
        "reconnect_seq",
        Some(FLAKY_REGION),
    )
    .await?;
    assert_eq!(count, TOTAL_ROWS as i64);

    Ok(())
}

/// Tests parallel appends to a table with the `_reconnect_on_close` suffix in `us-east7`.
///
/// BigQuery drops the connection every 10 requests to exercise concurrent
/// retry and reconnect handling.
pub async fn reconnect_on_close_parallel(
    client: &Write,
    table_service: &TableService,
    project_id: &str,
    dataset_id: &str,
    schema: TableSchema,
) -> Result<()> {
    let table_id = format!(
        "{}_reconnect_on_close_parallel",
        bigquery_samples::random_id_suffix()
    );
    create_flaky_table(table_service, project_id, dataset_id, &table_id, schema).await?;

    let table = format!("projects/{project_id}/datasets/{dataset_id}/tables/{table_id}");
    let mut serializer = ArrowSerializer::new("reconnect_par")?;

    // Pre-generate all batches
    let mut batches = Vec::with_capacity(ITERATIONS);
    for i in 0..ITERATIONS {
        let offset = (i * ROWS_PER_BATCH) as i64;
        let batch = serializer.generate_batch(ROWS_PER_BATCH, i * ROWS_PER_BATCH)?;
        batches.push((offset, batch));
    }

    let writer = Arc::new(client.arrow(serializer.schema()).pending(table).await?);

    let mut handles = Vec::with_capacity(ITERATIONS);
    for (offset, batch) in batches {
        let writer = writer.clone();
        handles.push(tokio::spawn(async move {
            let resp = writer.append(batch).set_offset(offset).send().await?;
            assert_eq!(resp.offset, Some(offset));
            Ok::<(), anyhow::Error>(())
        }));
    }

    for handle in handles {
        handle.await??;
    }

    let finalize_resp = writer.finalize().await?;
    assert_eq!(finalize_resp.row_count, TOTAL_ROWS as i64);

    let commit_resp = writer.commit().await?;
    assert!(
        commit_resp.stream_errors.is_empty(),
        "unexpected stream errors on commit: {:?}",
        commit_resp.stream_errors
    );

    let count = count_writes_table(
        project_id,
        dataset_id,
        &table_id,
        "reconnect_par",
        Some(FLAKY_REGION),
    )
    .await?;
    assert_eq!(count, TOTAL_ROWS as i64);

    Ok(())
}

/// Tests sequential appends to a table with the `_initial_connect_failure` suffix in `us-east7`.
///
/// BigQuery fails the initial connection more frequently on this table to exercise
/// stream connection retry behavior.
pub async fn initial_connect_failure_sequential(
    client: &Write,
    table_service: &TableService,
    project_id: &str,
    dataset_id: &str,
    schema: TableSchema,
) -> Result<()> {
    let table_id = format!(
        "{}_initial_connect_failure",
        bigquery_samples::random_id_suffix()
    );
    create_flaky_table(table_service, project_id, dataset_id, &table_id, schema).await?;

    let table = format!("projects/{project_id}/datasets/{dataset_id}/tables/{table_id}");
    let mut serializer = ArrowSerializer::new("init_fail_seq")?;

    let writer = client.arrow(serializer.schema()).pending(table).await?;

    for i in 0..ITERATIONS {
        let offset = (i * ROWS_PER_BATCH) as i64;
        let batch = serializer.generate_batch(ROWS_PER_BATCH, i * ROWS_PER_BATCH)?;
        let resp = writer.append(batch).set_offset(offset).send().await?;
        assert_eq!(resp.offset, Some(offset));
    }

    let finalize_resp = writer.finalize().await?;
    assert_eq!(finalize_resp.row_count, TOTAL_ROWS as i64);

    let commit_resp = writer.commit().await?;
    assert!(
        commit_resp.stream_errors.is_empty(),
        "unexpected stream errors on commit: {:?}",
        commit_resp.stream_errors
    );

    let count = count_writes_table(
        project_id,
        dataset_id,
        &table_id,
        "init_fail_seq",
        Some(FLAKY_REGION),
    )
    .await?;
    assert_eq!(count, TOTAL_ROWS as i64);

    Ok(())
}

/// Tests parallel appends to a table with the `_initial_connect_failure` suffix in `us-east7`.
///
/// BigQuery fails the initial connection more frequently on this table to exercise
/// concurrent stream connection retry behavior.
pub async fn initial_connect_failure_parallel(
    client: &Write,
    table_service: &TableService,
    project_id: &str,
    dataset_id: &str,
    schema: TableSchema,
) -> Result<()> {
    let table_id = format!(
        "{}_initial_connect_failure_parallel",
        bigquery_samples::random_id_suffix()
    );
    create_flaky_table(table_service, project_id, dataset_id, &table_id, schema).await?;

    let table = format!("projects/{project_id}/datasets/{dataset_id}/tables/{table_id}");
    let mut serializer = ArrowSerializer::new("init_fail_par")?;

    // Pre-generate all batches
    let mut batches = Vec::with_capacity(ITERATIONS);
    for i in 0..ITERATIONS {
        let offset = (i * ROWS_PER_BATCH) as i64;
        let batch = serializer.generate_batch(ROWS_PER_BATCH, i * ROWS_PER_BATCH)?;
        batches.push((offset, batch));
    }

    let writer = Arc::new(client.arrow(serializer.schema()).pending(table).await?);

    let mut handles = Vec::with_capacity(ITERATIONS);
    for (offset, batch) in batches {
        let writer = writer.clone();
        handles.push(tokio::spawn(async move {
            let resp = writer.append(batch).set_offset(offset).send().await?;
            assert_eq!(resp.offset, Some(offset));
            Ok::<(), anyhow::Error>(())
        }));
    }

    for handle in handles {
        handle.await??;
    }

    let finalize_resp = writer.finalize().await?;
    assert_eq!(finalize_resp.row_count, TOTAL_ROWS as i64);

    let commit_resp = writer.commit().await?;
    assert!(
        commit_resp.stream_errors.is_empty(),
        "unexpected stream errors on commit: {:?}",
        commit_resp.stream_errors
    );

    let count = count_writes_table(
        project_id,
        dataset_id,
        &table_id,
        "init_fail_par",
        Some(FLAKY_REGION),
    )
    .await?;
    assert_eq!(count, TOTAL_ROWS as i64);

    Ok(())
}

/// Tests default stream appends to a table with the `_reconnect_on_close` suffix in `us-east7`.
pub async fn reconnect_on_close_default(
    client: &Write,
    table_service: &TableService,
    project_id: &str,
    dataset_id: &str,
    schema: TableSchema,
) -> Result<()> {
    let table_id = format!("{}_reconnect_default", bigquery_samples::random_id_suffix());
    create_flaky_table(table_service, project_id, dataset_id, &table_id, schema).await?;

    let table = format!("projects/{project_id}/datasets/{dataset_id}/tables/{table_id}");
    let mut serializer = ArrowSerializer::new("reconnect_def")?;

    let writer = client.arrow(serializer.schema()).default(table).await?;

    for i in 0..ITERATIONS {
        let batch = serializer.generate_batch(ROWS_PER_BATCH, i * ROWS_PER_BATCH)?;
        let _ = writer.append(batch).send().await?;
    }

    let count = count_writes_table(
        project_id,
        dataset_id,
        &table_id,
        "reconnect_def",
        Some(FLAKY_REGION),
    )
    .await?;
    assert_eq!(count, TOTAL_ROWS as i64);

    Ok(())
}

async fn create_flaky_table(
    table_service: &TableService,
    project_id: &str,
    dataset_id: &str,
    table_id: &str,
    schema: TableSchema,
) -> Result<()> {
    println!("CREATING FLAKY TABLE WITH ID: {table_id} IN LOCATION: {FLAKY_REGION}");
    table_service
        .insert_table()
        .set_project_id(project_id)
        .set_dataset_id(dataset_id)
        .set_table(
            Table::new()
                .set_table_reference(
                    TableReference::new()
                        .set_project_id(project_id)
                        .set_dataset_id(dataset_id)
                        .set_table_id(table_id),
                )
                .set_location(FLAKY_REGION)
                .set_schema(schema),
        )
        .send()
        .await?;
    Ok(())
}
