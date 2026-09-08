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
use google_cloud_bigquery::model::{ArrowRecordBatch, ArrowSchema};
use google_cloud_bigquery::write::arrow::CommittedWriter;
use std::sync::Arc;

pub async fn basic(
    client: &Write,
    project_id: &str,
    dataset_id: &str,
    table_id: &str,
) -> Result<()> {
    let table = format!("projects/{project_id}/datasets/{dataset_id}/tables/{table_id}");
    let mut serializer = ArrowSerializer::new("basic")?;

    // Create a writer for the default stream
    let writer = client.arrow(serializer.schema()).default(table)?;

    // Write the batches
    let batch1 = serializer.batch(vec!["Alice", "Bob"], vec![25, 28])?;
    let _ = writer.append(batch1).send().await?;

    let batch2 = serializer.batch(vec!["Charlie"], vec![31])?;
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
    let mut serializer = ArrowSerializer::new("pending")?;

    // Create a writer for a pending stream
    let writer = client.arrow(serializer.schema()).pending(table).await?;

    // Write the batches
    let batch1 = serializer.batch(vec!["David", "Eve"], vec![42, 38])?;
    let resp1 = writer.append(batch1).set_offset(0).send().await?;
    assert_eq!(resp1.offset, Some(0));

    let batch2 = serializer.batch(vec!["Frank"], vec![55])?;
    let resp2 = writer.append(batch2).set_offset(2).send().await?;
    assert_eq!(resp2.offset, Some(2));

    // Finalize the stream
    writer.finalize().await?;

    // Verify no writes have been committed yet
    let users = read_writes_table(project_id, dataset_id, table_id, "pending").await?;
    assert!(users.is_empty(), "{users:?}");

    // Verify that appending to a finalized stream fails
    let batch3 = serializer.batch(vec!["Ghost"], vec![99])?;
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

pub async fn committed(
    client: &Write,
    project_id: &str,
    dataset_id: &str,
    table_id: &str,
) -> Result<()> {
    let table = format!("projects/{project_id}/datasets/{dataset_id}/tables/{table_id}");
    let mut serializer = ArrowSerializer::new("committed")?;

    // Create a writer for a committed stream
    let writer = client.arrow(serializer.schema()).committed(table).await?;

    // Write the batches
    let batch1 = serializer.batch(vec!["Gerald", "Hannah"], vec![20, 22])?;
    let resp1 = writer.append(batch1).set_offset(0).send().await?;
    assert_eq!(resp1.offset, Some(0));

    let batch2 = serializer.batch(vec!["Ian"], vec![24])?;
    let resp2 = writer.append(batch2).set_offset(2).send().await?;
    assert_eq!(resp2.offset, Some(2));

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
    let batch3 = serializer.batch(vec!["Jack"], vec![26])?;
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
    let mut serializer = ArrowSerializer::new("buffered")?;

    // Create a writer for a buffered stream
    let writer = client.arrow(serializer.schema()).buffered(table).await?;

    // Write the batches
    let batch1 = serializer.batch(vec!["Kelly", "Liam"], vec![30, 32])?;
    let resp1 = writer.append(batch1).set_offset(0).send().await?;
    assert_eq!(resp1.offset, Some(0));

    let batch2 = serializer.batch(vec!["Mia"], vec![34])?;
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
    let batch3 = serializer.batch(vec!["Noah"], vec![36])?;
    let _err = writer
        .append(batch3)
        .set_offset(3)
        .send()
        .await
        .expect_err("Appending to a finalized stream should fail");

    Ok(())
}

pub async fn attach(
    client: &Write,
    project_id: &str,
    dataset_id: &str,
    table_id: &str,
) -> Result<()> {
    let table = format!("projects/{project_id}/datasets/{dataset_id}/tables/{table_id}");
    let mut serializer = ArrowSerializer::new("attach")?;
    let schema = serializer.schema();

    let write_stream = {
        // Create a writer for a committed stream
        let writer = client.arrow(schema.clone()).committed(table).await?;

        // Write the first batch
        let batch1 = serializer.batch(vec!["Attached1", "Attached2"], vec![80, 81])?;
        let _ = writer.append(batch1).set_offset(0).send().await?;

        // Return the resource name of the write stream
        writer.write_stream().to_string()
    };

    // Attach to the previously created write stream from a new writer.
    let attached_writer: CommittedWriter = client.arrow(schema).attach(write_stream).await?;

    let batch2 = serializer.batch(vec!["Attached3"], vec![82])?;
    let _ = attached_writer.append(batch2).set_offset(2).send().await?;

    // Verify the writes are logically seamless across the boundary
    let users = read_writes_table(project_id, dataset_id, table_id, "attach").await?;
    assert_eq!(
        users,
        vec![
            WriteUserRecord {
                name: "Attached1".to_string(),
                age: 80,
                test: "attach".to_string()
            },
            WriteUserRecord {
                name: "Attached2".to_string(),
                age: 81,
                test: "attach".to_string()
            },
            WriteUserRecord {
                name: "Attached3".to_string(),
                age: 82,
                test: "attach".to_string()
            },
        ]
    );

    // Finalize via the attached client cleanly
    attached_writer.finalize().await?;

    Ok(())
}

struct ArrowSerializer {
    schema: Arc<Schema>,
    writer: StreamWriter<Vec<u8>>,
    test: &'static str,
}

impl ArrowSerializer {
    fn new(test: &'static str) -> Result<Self> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int64, false),
            Field::new("test", DataType::Utf8, false),
        ]));
        let writer = StreamWriter::try_new(Vec::new(), &schema)?;
        Ok(Self {
            schema,
            writer,
            test,
        })
    }

    fn schema(&mut self) -> ArrowSchema {
        let buf = std::mem::take(self.writer.get_mut());
        ArrowSchema::new().set_serialized_schema(buf)
    }

    fn batch(&mut self, names: Vec<&str>, ages: Vec<i64>) -> Result<ArrowRecordBatch> {
        let batch = {
            let name = StringArray::from(names);
            let age = Int64Array::from(ages);
            let test_col = StringArray::from(vec![self.test; age.len()]);
            RecordBatch::try_new(
                self.schema.clone(),
                vec![Arc::new(name), Arc::new(age), Arc::new(test_col)],
            )?
        };
        self.writer.write(&batch)?;
        let buf = std::mem::take(self.writer.get_mut());
        Ok(ArrowRecordBatch::new().set_serialized_record_batch(buf))
    }
}
