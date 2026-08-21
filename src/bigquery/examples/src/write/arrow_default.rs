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

// [START bigquerystorage_streamwriter_default_arrow]
use anyhow::Result;
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use google_cloud_bigquery::client::Write;
use google_cloud_bigquery::write::model::{ArrowRecordBatch, ArrowSchema};
use std::sync::Arc;
use tokio::task::JoinSet;

pub async fn sample(project_id: &str, dataset_id: &str, table_id: &str) -> anyhow::Result<()> {
    let client = Write::builder().build().await?;

    // Define the table schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("string", DataType::Utf8, false),
        Field::new("int", DataType::Int64, false),
    ]));

    let table = format!("projects/{project_id}/datasets/{dataset_id}/tables/{table_id}");
    // Create a writer for the default stream
    let writer = Arc::new(
        client
            .arrow(ArrowSchema::new().set_serialized_schema(serialize_schema(&schema)?))
            .default(table)?,
    );

    let mut writes = JoinSet::new();
    for i in 0..100 {
        let batch = make_batch(schema.clone(), i, 10)?;
        let writer = writer.clone();
        writes.spawn(async move { writer.append(batch).send().await });
    }
    let results: Result<Vec<_>, _> = writes.join_all().await.into_iter().collect();
    let _ = results?;
    println!("Successfully wrote 100 record batches of 10 rows each.");

    Ok(())
}

fn make_batch(schema: Arc<Schema>, index: i64, record_count: i64) -> Result<ArrowRecordBatch> {
    let schema_buf = serialize_schema(&schema)?;
    let schema_len = schema_buf.len();

    // Example data.
    let string = StringArray::from(vec![format!("batch {index}"); record_count as usize]);
    let int = Int64Array::from_iter_values(record_count * index..record_count * (index + 1));

    let batch = RecordBatch::try_new(schema, vec![Arc::new(string), Arc::new(int)])?;
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
    // Note that the schema is encoded in the front of the record batch, per the
    // IPC spec. BigQuery does not expect this, so we need to strip it.
    Ok(buf[schema_len..].to_vec())
}
// [END bigquerystorage_streamwriter_default_arrow]
