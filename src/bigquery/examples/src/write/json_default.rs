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

// [START bigquerystorage_jsonstreamwriter_default]
use anyhow::Result;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use arrow_json::ReaderBuilder;
use google_cloud_bigquery::client::Write;
use google_cloud_bigquery::model::{ArrowRecordBatch, ArrowSchema};
use std::sync::Arc;
use tokio::task::JoinSet;

// The client library does not natively support a JSON API surface (yet).
//
// This example demonstrates how to write JSON data to BigQuery by first
// converting it to Arrow record batches using the [arrow-json] crate.

pub async fn sample(project_id: &str, dataset_id: &str, table_id: &str) -> anyhow::Result<()> {
    let client = Write::builder().build().await?;

    // Define the table schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("string", DataType::Utf8, false),
        Field::new("int", DataType::Int64, false),
    ]));
    let schema_buf = serialize_schema(&schema)?;
    let schema_len = schema_buf.len();

    let table = format!("projects/{project_id}/datasets/{dataset_id}/tables/{table_id}");
    // Create a writer for the default stream
    let writer = client
        .arrow(ArrowSchema::new().set_serialized_schema(schema_buf))
        .default(table)?;

    // Create a decoder to convert JSON to Arrow
    let mut decoder = ReaderBuilder::new(schema).build_decoder()?;

    let mut writes = JoinSet::new();
    for i in 0..100 {
        for j in 0..10 {
            // Generate example data (JSON bytes).
            let json = format!(r#"{{"string": "batch {i}", "int": {}}}"#, 10 * i + j);
            let _ = decoder.decode(json.as_bytes())?;
        }
        if let Some(batch) = decoder.flush()? {
            let batch_buf = serialize_batch(&batch, schema_len)?;
            let batch = ArrowRecordBatch::new().set_serialized_record_batch(batch_buf);
            writes.spawn(writer.append(batch).send());
        }
    }
    let results: Result<Vec<_>, _> = writes.join_all().await.into_iter().collect();
    let _ = results?;
    println!("Successfully wrote 100 record batches of 10 rows each.");

    Ok(())
}

fn serialize_schema(schema: &Schema) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    let _ = StreamWriter::try_new(&mut buf, schema)?;
    Ok(buf)
}

fn serialize_batch(batch: &RecordBatch, schema_len: usize) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &batch.schema())?;
        writer.write(batch)?;
    }
    // Note that the schema is encoded in the front of the record batch, per the
    // IPC spec. BigQuery does not expect this, so we need to strip it.
    buf.drain(0..schema_len);
    Ok(buf)
}
// [END bigquerystorage_jsonstreamwriter_default]
