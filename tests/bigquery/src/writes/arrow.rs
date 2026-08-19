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
use google_cloud_bigquery_write::client::Write;
use google_cloud_bigquery_write::model::{ArrowRecordBatch, ArrowSchema};
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

    // Finalize the stream and commit
    writer.finalize().await?;
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
