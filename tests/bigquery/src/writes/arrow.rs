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

use crate::writes::{WriteUserRecord, read_writes_table};
use ::arrow::array::{Int64Array, StringArray};
use ::arrow::datatypes::{DataType, Field, Schema};
use ::arrow::ipc::writer::StreamWriter;
use ::arrow::record_batch::RecordBatch;
use anyhow::Result;
use google_cloud_bigquery::client::Write;
use google_cloud_bigquery::write::model::{ArrowRecordBatch, ArrowSchema};
use std::sync::Arc;

pub async fn basic(
    client: &Write,
    project_id: &str,
    dataset_id: &str,
    table_id: &str,
) -> Result<()> {
    let table = format!("projects/{project_id}/datasets/{dataset_id}/tables/{table_id}");
    let schema = create_test_schema();

    // Create a writer for the default stream
    let writer = client
        .arrow(ArrowSchema::new().set_serialized_schema(serialize_schema(&schema)?))
        .default(table)?;

    // Write the batches
    let batch1 = create_test_batch(schema.clone(), vec!["Alice", "Bob"], vec![25, 28], "basic")?;
    let _ = writer.append(batch1).send().await?;

    let batch2 = create_test_batch(schema.clone(), vec!["Charlie"], vec![31], "basic")?;
    let _ = writer.append(batch2).send().await?;

    // Verify the writes
    let users = read_writes_table(project_id, dataset_id, table_id, "basic").await?;
    assert_eq!(
        users,
        vec![
            WriteUserRecord {
                name: "Alice".to_string(),
                age: 25,
                test: "basic".to_string()
            },
            WriteUserRecord {
                name: "Bob".to_string(),
                age: 28,
                test: "basic".to_string()
            },
            WriteUserRecord {
                name: "Charlie".to_string(),
                age: 31,
                test: "basic".to_string()
            },
        ]
    );

    Ok(())
}

pub async fn pending(
    client: &Write,
    project_id: &str,
    dataset_id: &str,
    table_id: &str,
) -> Result<()> {
    let table = format!("projects/{project_id}/datasets/{dataset_id}/tables/{table_id}");
    let schema = create_test_schema();

    // Create a writer for a pending stream
    let writer = client
        .arrow(ArrowSchema::new().set_serialized_schema(serialize_schema(&schema)?))
        .pending(table)
        .await?;

    // Write the batches
    let batch1 = create_test_batch(
        schema.clone(),
        vec!["David", "Eve"],
        vec![42, 38],
        "pending",
    )?;
    let _ = writer.append(batch1).set_offset(0).send().await?;

    let batch2 = create_test_batch(schema.clone(), vec!["Frank"], vec![55], "pending")?;
    let _ = writer.append(batch2).set_offset(2).send().await?;

    // Finalize the stream
    writer.finalize().await?;

    // Verify no writes have been committed yet
    let users = read_writes_table(project_id, dataset_id, table_id, "pending").await?;
    assert!(users.is_empty(), "{users:?}");

    // Verify that appending to a finalized stream fails
    let batch3 = create_test_batch(schema.clone(), vec!["Ghost"], vec![99], "pending")?;
    let _err = writer
        .append(batch3)
        .set_offset(3)
        .send()
        .await
        .expect_err("Appending to a finalized stream should fail");
    // Commit the stream
    writer.commit().await?;

    // Verify the writes
    let users = read_writes_table(project_id, dataset_id, table_id, "pending").await?;
    assert_eq!(
        users,
        vec![
            WriteUserRecord {
                name: "David".to_string(),
                age: 42,
                test: "pending".to_string()
            },
            WriteUserRecord {
                name: "Eve".to_string(),
                age: 38,
                test: "pending".to_string()
            },
            WriteUserRecord {
                name: "Frank".to_string(),
                age: 55,
                test: "pending".to_string()
            },
        ]
    );

    Ok(())
}
fn create_test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int64, false),
        Field::new("test", DataType::Utf8, false),
    ]))
}

fn create_test_batch(
    schema: Arc<Schema>,
    names: Vec<&str>,
    ages: Vec<i64>,
    test: &str,
) -> Result<ArrowRecordBatch> {
    let schema_buf = serialize_schema(&schema)?;
    let schema_len = schema_buf.len();

    let name = StringArray::from(names);
    let age = Int64Array::from(ages);
    let test_col = StringArray::from(vec![test; age.len()]);

    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(name), Arc::new(age), Arc::new(test_col)],
    )?;
    let batch_buf = serialize_batch(&batch, schema_len)?;

    Ok(ArrowRecordBatch::new().set_serialized_record_batch(batch_buf))
}

fn serialize_schema(schema: &Schema) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    let _ = StreamWriter::try_new(&mut buf, schema)?;
    Ok(buf)
}

fn serialize_batch(batch: &RecordBatch, schema_len: usize) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, &batch.schema())?;
    writer.write(batch)?;
    // Note that the schema is encoded in the front of the record batch. We need
    // to strip it.
    Ok(buf[schema_len..].to_vec())
}

pub async fn committed(
    client: &Write,
    project_id: &str,
    dataset_id: &str,
    table_id: &str,
) -> Result<()> {
    let table = format!("projects/{project_id}/datasets/{dataset_id}/tables/{table_id}");
    let schema = create_test_schema();

    // Create a writer for a committed stream
    let writer = client
        .arrow(ArrowSchema::new().set_serialized_schema(serialize_schema(&schema)?))
        .committed(table)
        .await?;

    // Write the batches
    let batch1 = create_test_batch(
        schema.clone(),
        vec!["Gerald", "Hannah"],
        vec![20, 22],
        "committed",
    )?;
    let _ = writer.append(batch1).send().await?;

    let batch2 = create_test_batch(schema.clone(), vec!["Ian"], vec![24], "committed")?;
    let _ = writer.append(batch2).send().await?;

    // Verify the writes are immediately available since it's a committed stream
    let users = read_writes_table(project_id, dataset_id, table_id, "committed").await?;
    assert_eq!(
        users,
        vec![
            WriteUserRecord {
                name: "Gerald".to_string(),
                age: 20,
                test: "committed".to_string()
            },
            WriteUserRecord {
                name: "Hannah".to_string(),
                age: 22,
                test: "committed".to_string()
            },
            WriteUserRecord {
                name: "Ian".to_string(),
                age: 24,
                test: "committed".to_string()
            },
        ]
    );

    // Finalize the stream
    writer.finalize().await?;

    // Verify that appending to a finalized stream fails
    let batch3 = create_test_batch(schema.clone(), vec!["Jack"], vec![26], "committed")?;
    let _err = writer
        .append(batch3)
        .send()
        .await
        .expect_err("Appending to a finalized stream should fail");

    Ok(())
}

pub async fn buffered(
    client: &Write,
    project_id: &str,
    dataset_id: &str,
    table_id: &str,
) -> Result<()> {
    let table = format!("projects/{project_id}/datasets/{dataset_id}/tables/{table_id}");
    let schema = create_test_schema();

    // Create a writer for a buffered stream
    let writer = client
        .arrow(ArrowSchema::new().set_serialized_schema(serialize_schema(&schema)?))
        .buffered(table)
        .await?;

    // Write the batches (no implicit commit yet)
    let batch1 = create_test_batch(
        schema.clone(),
        vec!["Kelly", "Liam"],
        vec![30, 32],
        "buffered",
    )?;
    let resp1 = writer.append(batch1).set_offset(0).send().await?;
    assert_eq!(resp1.offset, Some(0));

    let batch2 = create_test_batch(schema.clone(), vec!["Mia"], vec![34], "buffered")?;
    let resp2 = writer.append(batch2).set_offset(2).send().await?;
    assert_eq!(resp2.offset, Some(2));

    // Verify no writes have been committed
    let users = read_writes_table(project_id, dataset_id, table_id, "buffered").await?;
    assert!(users.is_empty(), "{users:?}");

    // Flush to offset 1 (Kelly and Liam)
    let flush1 = writer.flush(1).await?;
    assert_eq!(flush1.offset, 1);

    // Verify first batch is visible
    let users = read_writes_table(project_id, dataset_id, table_id, "buffered").await?;
    assert_eq!(
        users,
        vec![
            WriteUserRecord {
                name: "Kelly".to_string(),
                age: 30,
                test: "buffered".to_string()
            },
            WriteUserRecord {
                name: "Liam".to_string(),
                age: 32,
                test: "buffered".to_string()
            },
        ]
    );

    // Flush to offset 2 (Mia)
    let flush2 = writer.flush(2).await?;
    assert_eq!(flush2.offset, 2);

    let users = read_writes_table(project_id, dataset_id, table_id, "buffered").await?;
    assert_eq!(
        users,
        vec![
            WriteUserRecord {
                name: "Kelly".to_string(),
                age: 30,
                test: "buffered".to_string()
            },
            WriteUserRecord {
                name: "Liam".to_string(),
                age: 32,
                test: "buffered".to_string()
            },
            WriteUserRecord {
                name: "Mia".to_string(),
                age: 34,
                test: "buffered".to_string()
            },
        ]
    );

    // Finalize the stream
    writer.finalize().await?;

    // Verify that appending to a finalized stream fails
    let batch3 = create_test_batch(schema.clone(), vec!["Noah"], vec![36], "buffered")?;
    let _err = writer
        .append(batch3)
        .set_offset(3)
        .send()
        .await
        .expect_err("Appending to a finalized stream should fail");

    Ok(())
}
