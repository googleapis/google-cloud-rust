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

use super::*;
use ::arrow::array::{Int64Array, StringArray};
use ::arrow::datatypes::{DataType, Field, Schema};
use ::arrow::ipc::writer::StreamWriter;
use ::arrow::record_batch::RecordBatch;
use google_cloud_bigquery_write::client::Write;
use google_cloud_bigquery_write::model::{ArrowRecordBatch, ArrowSchema};
use std::sync::Arc;

pub async fn basic(project_id: &str, dataset_id: &str, table_id: &str) -> Result<()> {
    let table = format!("projects/{project_id}/datasets/{dataset_id}/tables/{table_id}");

    // Create a Schema
    let arrow_schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int64, false),
    ]));
    let schema_buf = serialize_schema(&arrow_schema)?;
    let schema_len = schema_buf.len();

    // Create a writer for the default stream
    let client = Write::builder().build().await?;
    let schema = ArrowSchema::new().set_serialized_schema(schema_buf);
    let writer = client.arrow(schema).default(table)?;

    // Create a RecordBatch
    let name = StringArray::from(vec!["Alice", "Bob"]);
    let age = Int64Array::from(vec![25, 28]);
    let batch = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(name), Arc::new(age)])?;
    let batch_buf = serialize_batch(&batch, schema_len)?;

    // Write the batch
    let rows = ArrowRecordBatch::new().set_serialized_record_batch(batch_buf);
    let _ = writer.append(rows).send().await?;

    // Create a second RecordBatch
    let name = StringArray::from(vec!["Charlie"]);
    let age = Int64Array::from(vec![31]);
    let batch = RecordBatch::try_new(arrow_schema, vec![Arc::new(name), Arc::new(age)])?;
    let batch_buf = serialize_batch(&batch, schema_len)?;

    // Write the second batch
    let rows = ArrowRecordBatch::new().set_serialized_record_batch(batch_buf);
    let _ = writer.append(rows).send().await?;

    // Verify the writes
    let users = read_table(project_id, dataset_id, &table_id).await?;
    assert_eq!(
        users,
        vec![
            UserRecord {
                name: "Alice".to_string(),
                age: 25,
            },
            UserRecord {
                name: "Bob".to_string(),
                age: 28,
            },
            UserRecord {
                name: "Charlie".to_string(),
                age: 31,
            },
        ]
    );

    Ok(())
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
    Ok(buf[schema_len..].to_vec())
}
