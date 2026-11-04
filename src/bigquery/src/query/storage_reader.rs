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

use crate::RequestOptions;
use crate::client::Read as ReadClient;
use crate::error::RowError;
use crate::model::{
    ArrowSerializationOptions, CreateReadSessionRequest, DataFormat, ReadRowsResponse, ReadSession,
    arrow_serialization_options::CompressionCodec, read_rows_response::Rows,
    read_rows_response::Schema as ReadRowsResponseSchema,
    read_session::Schema as ReadSessionSchema, read_session::TableReadOptions,
};
use crate::query::query_handle::build_get_job;
use arrow::record_batch::RecordBatch;
use google_cloud_bigquery_v2::client::JobService;
use google_cloud_bigquery_v2::model::JobReference;
use google_cloud_gax::streaming::ResponseStream;
use std::sync::Arc;

/// Default threshold below which queries stick to REST pagination.
pub(crate) const DEFAULT_REST_ROW_THRESHOLD: u64 = 10_000;

// TODO(#7038): remove environment variable override once benchmarks establish the optimal threshold.
pub(crate) fn rest_row_threshold() -> u64 {
    std::env::var("GOOGLE_CLOUD_RUST_BIGQUERY_REST_THRESHOLD")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_REST_ROW_THRESHOLD)
}

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

// TODO(#7039): wire into StorageReader once concurrent stream reading lands.
#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ReadSessionInfo {
    pub(crate) streams: Vec<String>,
    pub(crate) arrow_schema: Option<bytes::Bytes>,
}

// TODO(#7039): wire into StorageReader once concurrent stream reading lands.
#[allow(dead_code)]
pub(crate) async fn try_create_read_session(
    read_client: &ReadClient,
    job_service: &JobService,
    job_ref: &JobReference,
    options: &RequestOptions,
) -> Result<Option<ReadSessionInfo>, RowError> {
    let Some(get_job) = build_get_job(job_service, job_ref, options) else {
        return Ok(None);
    };

    let job = get_job.send().await?;

    let dest_table = job
        .configuration
        .and_then(|c| c.query)
        .and_then(|q| q.destination_table);

    let Some(dest_table) = dest_table else {
        return Ok(None);
    };

    if dest_table.project_id.is_empty()
        || dest_table.dataset_id.is_empty()
        || dest_table.table_id.is_empty()
    {
        return Ok(None);
    }

    let table_path = format!(
        "projects/{}/datasets/{}/tables/{}",
        dest_table.project_id, dest_table.dataset_id, dest_table.table_id
    );

    let session_req = CreateReadSessionRequest::new()
        .set_parent(format!("projects/{}", job_ref.project_id))
        .set_read_session(
            ReadSession::new()
                .set_table(table_path)
                .set_data_format(DataFormat::Arrow)
                .set_read_options(
                    TableReadOptions::new().set_arrow_serialization_options(
                        ArrowSerializationOptions::new()
                            .set_buffer_compression(CompressionCodec::Lz4Frame),
                    ),
                ),
        );

    let session = read_client
        .create_read_session()
        .with_options(options.clone())
        .with_request(session_req)
        .send()
        .await?;

    let arrow_schema = match session.schema {
        Some(ReadSessionSchema::ArrowSchema(schema)) if !schema.serialized_schema.is_empty() => {
            Some(schema.serialized_schema)
        }
        _ => None,
    };
    let streams = session.streams.into_iter().map(|s| s.name).collect();
    Ok(Some(ReadSessionInfo {
        streams,
        arrow_schema,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ArrowRecordBatch, ArrowSchema, ReadStream};
    use crate::query::arrow::RecordBatchSource;
    use crate::query::tests::{MockJobService, MockRead, create_job_service};
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchemaDef};
    use arrow::ipc::writer::StreamWriter;
    use google_cloud_bigquery_v2::model::{
        Job, JobConfiguration, JobConfigurationQuery, TableReference,
    };
    use google_cloud_gax::error::Error as GaxError;
    use google_cloud_gax::error::rpc::{Code, Status};
    use google_cloud_gax::response::Response;
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

    fn job_with_dest_table(project_id: &str, dataset_id: &str, table_id: &str) -> Job {
        Job::new().set_configuration(
            JobConfiguration::new().set_query(
                JobConfigurationQuery::new().set_destination_table(
                    TableReference::new()
                        .set_project_id(project_id)
                        .set_dataset_id(dataset_id)
                        .set_table_id(table_id),
                ),
            ),
        )
    }

    #[test_case::test_case(0; "zero streams")]
    #[test_case::test_case(1; "single stream")]
    #[test_case::test_case(2; "multiple streams")]
    #[tokio::test]
    async fn test_try_create_read_session_creates_arrow_lz4_session(
        stream_count: usize,
    ) -> TestResult {
        let mut mock_job = MockJobService::new();
        mock_job.expect_get_job().times(1).returning(|req, opts| {
            assert_eq!(req.project_id, "billing-proj");
            assert_eq!(req.job_id, "job-123");
            assert_eq!(req.location, "us-central1");
            assert_eq!(opts.quota_project().as_deref(), Some("quota-proj"));
            Ok(Response::from(job_with_dest_table(
                "dest-proj",
                "_anon_ds",
                "anon_tbl",
            )))
        });

        let expected_streams: Vec<String> = (0..stream_count)
            .map(|i| format!("sessions/s1/streams/{i}"))
            .collect();
        let streams_for_mock = expected_streams.clone();
        let schema_bytes = bytes::Bytes::from_static(b"arrow-schema-bytes");
        let expected_schema = schema_bytes.clone();
        let mut mock_read = MockRead::new();
        mock_read
            .expect_create_read_session()
            .times(1)
            .return_once(move |req, opts| {
                assert_eq!(req.parent, "projects/billing-proj");
                assert_eq!(opts.quota_project().as_deref(), Some("quota-proj"));
                let session_req = req.read_session.expect("read_session must be set");
                assert_eq!(
                    session_req.table,
                    "projects/dest-proj/datasets/_anon_ds/tables/anon_tbl"
                );
                assert_eq!(session_req.data_format, DataFormat::Arrow);
                let arrow_opts = session_req
                    .read_options
                    .and_then(|o| o.arrow_serialization_options().cloned())
                    .expect("arrow_serialization_options must be set");
                assert_eq!(arrow_opts.buffer_compression, CompressionCodec::Lz4Frame);

                let streams = streams_for_mock
                    .into_iter()
                    .map(|name| ReadStream::new().set_name(name));
                let session = ReadSession::new()
                    .set_arrow_schema(ArrowSchema::new().set_serialized_schema(schema_bytes))
                    .set_streams(streams);
                Ok(Response::from(session))
            });

        let job_service = create_job_service(mock_job);
        let read_client = crate::client::Read::from_stub(mock_read);
        let job_ref = JobReference::new()
            .set_project_id("billing-proj")
            .set_location("us-central1")
            .set_job_id("job-123");
        let mut options = RequestOptions::default();
        options.set_quota_project("quota-proj");

        let result =
            try_create_read_session(&read_client, &job_service, &job_ref, &options).await?;

        assert_eq!(
            result,
            Some(ReadSessionInfo {
                streams: expected_streams,
                arrow_schema: Some(expected_schema),
            })
        );
        Ok(())
    }

    #[test_case::test_case(JobReference::new(), None; "empty job reference")]
    #[test_case::test_case(JobReference::new().set_project_id("p1").set_job_id("j1"), Some(Job::new()); "no destination table")]
    #[test_case::test_case(JobReference::new().set_project_id("p1").set_job_id("j1"), Some(job_with_dest_table("p1", "ds1", "")); "empty destination table id")]
    #[tokio::test]
    async fn test_try_create_read_session_returns_none_without_destination_table(
        job_ref: JobReference,
        job_response: Option<Job>,
    ) -> TestResult {
        let mut mock_job = MockJobService::new();
        match job_response {
            Some(job) => {
                mock_job
                    .expect_get_job()
                    .times(1)
                    .return_once(move |_, _| Ok(Response::from(job)));
            }
            None => {
                mock_job.expect_get_job().never();
            }
        }

        let mut mock_read = MockRead::new();
        mock_read.expect_create_read_session().never();

        let job_service = create_job_service(mock_job);
        let read_client = crate::client::Read::from_stub(mock_read);
        let result = try_create_read_session(
            &read_client,
            &job_service,
            &job_ref,
            &RequestOptions::default(),
        )
        .await?;

        assert_eq!(result, None);
        Ok(())
    }

    #[test_case::test_case(true; "get_job rpc error")]
    #[test_case::test_case(false; "create_read_session rpc error")]
    #[tokio::test]
    async fn test_try_create_read_session_propagates_rpc_errors(fail_get_job: bool) -> TestResult {
        let mut mock_job = MockJobService::new();
        let mut mock_read = MockRead::new();
        let denied = |msg: &str| {
            GaxError::service(
                Status::default()
                    .set_code(Code::PermissionDenied)
                    .set_message(msg),
            )
        };

        if fail_get_job {
            mock_job
                .expect_get_job()
                .times(1)
                .returning(move |_, _| Err(denied("get_job denied")));
            mock_read.expect_create_read_session().never();
        } else {
            mock_job
                .expect_get_job()
                .times(1)
                .returning(|_, _| Ok(Response::from(job_with_dest_table("p1", "ds1", "t1"))));
            mock_read
                .expect_create_read_session()
                .times(1)
                .returning(move |_, _| Err(denied("create_read_session denied")));
        }

        let job_service = create_job_service(mock_job);
        let read_client = crate::client::Read::from_stub(mock_read);
        let job_ref = JobReference::new().set_project_id("p1").set_job_id("j1");
        let err = try_create_read_session(
            &read_client,
            &job_service,
            &job_ref,
            &RequestOptions::default(),
        )
        .await
        .expect_err("should propagate RPC error");

        assert!(matches!(err, RowError::Rpc { .. }), "{err:?}");
        Ok(())
    }
}
