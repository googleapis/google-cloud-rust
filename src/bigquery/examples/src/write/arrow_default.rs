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
use google_cloud_bigquery::model::{ArrowRecordBatch, ArrowSchema};
use std::sync::Arc;
use tokio::task::JoinSet;

pub async fn sample(project_id: &str, dataset_id: &str, table_id: &str) -> anyhow::Result<()> {
    let client = Write::builder().build().await?;

    // Define the table schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("string", DataType::Utf8, false),
        Field::new("int", DataType::Int64, false),
    ]));

    // Initialize an IPC stream writer and extract the serialized schema
    let mut ipc_writer = StreamWriter::try_new(Vec::new(), &schema)?;
    let schema_buf = std::mem::take(ipc_writer.get_mut());

    let table = format!("projects/{project_id}/datasets/{dataset_id}/tables/{table_id}");
    // Create a writer for the default stream
    let writer = client
        .arrow(ArrowSchema::new().set_serialized_schema(schema_buf))
        .default(table)?;

    let mut writes = JoinSet::new();
    for i in 0..100 {
        // Generate example data (`arrow::RecordBatch`).
        let batch = {
            let string = StringArray::from(vec![format!("batch {i}"); 10]);
            let int = Int64Array::from_iter_values(10 * i..10 * (i + 1));
            RecordBatch::try_new(schema.clone(), vec![Arc::new(string), Arc::new(int)])?
        };
        // Serialize the batch
        let batch = {
            ipc_writer.write(&batch)?;
            let batch_bytes = std::mem::take(ipc_writer.get_mut());
            ArrowRecordBatch::new().set_serialized_record_batch(batch_bytes)
        };
        // Write the batch to BigQuery
        writes.spawn(writer.append(batch).send());
    }
    let results: Result<Vec<_>, _> = writes.join_all().await.into_iter().collect();
    let _ = results?;
    println!("Successfully wrote 100 record batches of 10 rows each.");

    Ok(())
}
// [END bigquerystorage_streamwriter_default_arrow]
