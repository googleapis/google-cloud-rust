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

use crate::client::Read as ReadClient;
use crate::error::RowError;
use crate::model::{
    ReadRowsResponse, read_rows_response::Rows,
    read_rows_response::Schema as ReadRowsResponseSchema,
};
use arrow::record_batch::RecordBatch;
use google_cloud_bigquery_v2::model::JobReference;
use google_cloud_gax::streaming::ResponseStream;
use std::sync::Arc;

#[allow(dead_code)]
#[derive(Debug)]
pub(crate) enum ReaderState {
    Unopened,
    Streaming {
        stream: ResponseStream<ReadRowsResponse>,
    },
    Completed,
}

/// Reads query results via a single Storage Read API stream (`_default`).
#[derive(Debug)]
pub(crate) struct StorageReader {
    pub(crate) read_client: Arc<ReadClient>,
    pub(crate) job_ref: JobReference,
    pub(crate) decoder: crate::query::arrow::ArrowStreamDecoder,
    pub(crate) state: ReaderState,
}

impl StorageReader {
    #[allow(dead_code)]
    pub(crate) fn new(read_client: Arc<ReadClient>, job_ref: JobReference) -> Self {
        Self {
            read_client,
            job_ref,
            decoder: crate::query::arrow::ArrowStreamDecoder::new(),
            state: ReaderState::Unopened,
        }
    }
}

impl crate::query::arrow::RecordBatchSource for StorageReader {
    async fn next_record_batch(&mut self) -> Result<Option<RecordBatch>, RowError> {
        loop {
            match &mut self.state {
                ReaderState::Completed => return Ok(None),
                ReaderState::Unopened => {
                    let project = &self.job_ref.project_id;
                    let location = self.job_ref.location.as_deref().unwrap_or("US");
                    let job_id = &self.job_ref.job_id;
                    let stream_name = format!(
                        "projects/{project}/locations/{location}/jobs/{job_id}/streams/_default"
                    );
                    let stream = match self
                        .read_client
                        .read_rows()
                        .set_read_stream(stream_name)
                        .set_offset(0)
                        .send()
                        .await
                    {
                        Ok(stream) => stream,
                        Err(err) => {
                            self.state = ReaderState::Completed;
                            return Err(RowError::Rpc { source: err });
                        }
                    };

                    self.state = ReaderState::Streaming { stream };
                }
                ReaderState::Streaming { stream } => match stream.next().await {
                    Some(Ok(response)) => {
                        if let Some(ReadRowsResponseSchema::ArrowSchema(arrow_schema)) =
                            response.schema
                            && let Err(err) = self
                                .decoder
                                .set_schema_bytes(arrow_schema.serialized_schema)
                        {
                            self.state = ReaderState::Completed;
                            return Err(err);
                        }

                        if let Some(Rows::ArrowRecordBatch(batch)) = response.rows {
                            match self.decoder.decode_batch(batch.serialized_record_batch) {
                                Ok(Some(record_batch)) => {
                                    if record_batch.num_rows() > 0 {
                                        return Ok(Some(record_batch));
                                    }
                                }
                                Ok(None) => {}
                                Err(err) => {
                                    self.state = ReaderState::Completed;
                                    return Err(err);
                                }
                            }
                        }
                    }
                    None => {
                        self.state = ReaderState::Completed;
                        return Ok(None);
                    }
                    Some(Err(err)) => {
                        self.state = ReaderState::Completed;
                        return Err(RowError::Rpc { source: err });
                    }
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ArrowRecordBatch, ArrowSchema};
    use crate::query::arrow::RecordBatchSource;
    use crate::query::tests::MockRead;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchemaDef};
    use arrow::ipc::writer::StreamWriter;
    use google_cloud_gax::error::Error as GaxError;
    use google_cloud_gax::error::rpc::{Code, Status};
    use tokio::sync::mpsc;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    #[tokio::test]
    async fn test_storage_reader_reads_batches_successfully() -> TestResult {
        let arrow_schema = Arc::new(ArrowSchemaDef::new(vec![Field::new(
            "number",
            DataType::Int64,
            false,
        )]));

        let mut schema_buf = Vec::new();
        let _ = StreamWriter::try_new(&mut schema_buf, &arrow_schema)?;

        let col = Int64Array::from(vec![1, 2, 3]);
        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(col)])?;

        let mut batch_buf = Vec::new();
        let mut writer = StreamWriter::try_new(&mut batch_buf, &arrow_schema)?;
        writer.write(&batch)?;
        let batch_buf = batch_buf[schema_buf.len()..].to_vec();

        let (tx, rx) = mpsc::channel(2);
        // First message: schema only
        tx.send(Ok(ReadRowsResponse::default().set_schema(
            ReadRowsResponseSchema::ArrowSchema(Box::new(
                ArrowSchema::default().set_serialized_schema(bytes::Bytes::from(schema_buf)),
            )),
        )))
        .await?;
        // Second message: rows
        tx.send(Ok(ReadRowsResponse::default().set_rows(
            Rows::ArrowRecordBatch(Box::new(
                ArrowRecordBatch::default()
                    .set_serialized_record_batch(bytes::Bytes::from(batch_buf)),
            )),
        )))
        .await?;
        drop(tx);

        let mut mock_read = MockRead::new();
        mock_read
            .expect_read_rows()
            .times(1)
            .return_once(move |req, _| {
                assert_eq!(
                    req.read_stream,
                    "projects/p1/locations/loc1/jobs/j1/streams/_default"
                );
                assert_eq!(req.offset, 0);
                Ok(ResponseStream::from(rx))
            });

        let read_client = Arc::new(crate::client::Read::from_stub(mock_read));
        let job_ref = JobReference::default()
            .set_project_id("p1")
            .set_location("loc1")
            .set_job_id("j1");

        let mut reader = StorageReader::new(read_client, job_ref);

        let batch1 = reader
            .next_record_batch()
            .await?
            .expect("should yield batch");
        assert_eq!(batch1.num_rows(), 3);
        assert_eq!(
            batch1
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[1, 2, 3]
        );

        let batch2 = reader.next_record_batch().await?;
        assert!(batch2.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_storage_reader_handles_rpc_error() -> TestResult {
        let mut mock_read = MockRead::new();
        mock_read.expect_read_rows().times(1).returning(|_, _| {
            let status = Status::default()
                .set_code(Code::PermissionDenied)
                .set_message("permission denied on stream");
            Err(GaxError::service(status))
        });

        let read_client = Arc::new(crate::client::Read::from_stub(mock_read));
        let job_ref = JobReference::default()
            .set_project_id("p1")
            .set_job_id("j1");

        let mut reader = StorageReader::new(read_client, job_ref);
        let err = reader.next_record_batch().await.unwrap_err();
        assert!(matches!(err, RowError::Rpc { .. }), "{err:?}");
        assert!(err.to_string().contains("permission denied on stream"));

        // After error, reader is Completed and returns None
        assert!(reader.next_record_batch().await?.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_storage_reader_empty_stream() -> TestResult {
        let (tx, rx) = mpsc::channel(1);
        drop(tx);

        let mut mock_read = MockRead::new();
        mock_read
            .expect_read_rows()
            .times(1)
            .return_once(move |_, _| Ok(ResponseStream::from(rx)));

        let read_client = Arc::new(crate::client::Read::from_stub(mock_read));
        let job_ref = JobReference::default()
            .set_project_id("p1")
            .set_job_id("j1");

        let mut reader = StorageReader::new(read_client, job_ref);
        assert!(reader.next_record_batch().await?.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_storage_reader_handles_decoding_error_and_transitions_to_completed() -> TestResult
    {
        let (tx, rx) = mpsc::channel(1);
        tx.send(Ok(ReadRowsResponse::default().set_schema(
            ReadRowsResponseSchema::ArrowSchema(Box::new(
                ArrowSchema::default().set_serialized_schema(bytes::Bytes::from_static(
                    b"\xff\xff\xff\xff\x08\x00\x00\x00invalid_schema",
                )),
            )),
        )))
        .await?;
        drop(tx);

        let mut mock_read = MockRead::new();
        mock_read
            .expect_read_rows()
            .times(1)
            .return_once(move |_, _| Ok(ResponseStream::from(rx)));

        let read_client = Arc::new(crate::client::Read::from_stub(mock_read));
        let job_ref = JobReference::default()
            .set_project_id("p1")
            .set_job_id("j1");

        let mut reader = StorageReader::new(read_client, job_ref);
        let err = reader.next_record_batch().await.unwrap_err();
        assert!(matches!(err, RowError::InvalidRowFormat(_)), "{err:?}");

        // After decoding error, reader must be Completed and return None
        assert!(reader.next_record_batch().await?.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_storage_reader_handles_batch_decoding_error_and_transitions_to_completed()
    -> TestResult {
        let (tx, rx) = mpsc::channel(1);
        tx.send(Ok(ReadRowsResponse::default().set_rows(
            Rows::ArrowRecordBatch(Box::new(
                ArrowRecordBatch::default().set_serialized_record_batch(bytes::Bytes::from_static(
                    b"\xff\xff\xff\xff\x08\x00\x00\x00invalid_batch",
                )),
            )),
        )))
        .await?;
        drop(tx);

        let mut mock_read = MockRead::new();
        mock_read
            .expect_read_rows()
            .times(1)
            .return_once(move |_, _| Ok(ResponseStream::from(rx)));

        let read_client = Arc::new(crate::client::Read::from_stub(mock_read));
        let job_ref = JobReference::default()
            .set_project_id("p1")
            .set_job_id("j1");

        let mut reader = StorageReader::new(read_client, job_ref);
        let err = reader.next_record_batch().await.unwrap_err();
        assert!(matches!(err, RowError::InvalidRowFormat(_)), "{err:?}");

        // After decoding error, reader must be Completed and return None
        assert!(reader.next_record_batch().await?.is_none());

        Ok(())
    }
}
